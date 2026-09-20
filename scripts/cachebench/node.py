#!/usr/bin/env python3
"""Kind node-local process accounting, scoped page reclamation and perf capture."""

import collections
import errno
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.parse
import urllib.request


def processes():
    result = {}
    for path in Path('/proc').glob('[0-9]*'):
        try:
            args = path.joinpath('cmdline').read_bytes().split(b'\0')
            name = Path(os.fsdecode(args[0])).name
            if name == 'cachebench' and b'meter' not in args:
                env = dict(s.split(b'=', 1) for s in path.joinpath('environ').read_bytes().split(b'\0') if b'=' in s)
                label = env[b'BACKEND'].decode() + '-' + env[b'HOSTNAME'].decode()
            elif name == 'rclone' and b'mount' in args:
                label = 'rclone-mount'
            elif name in ('mount-s3', 'aws-s3-csi-mounter'):
                label = 'mountpoint-mount'
            elif name == 's3cache':
                label = 'proxy-s3cache'
            else:
                continue
            group = path.joinpath('cgroup').read_text().strip().split('::', 1)[1]
            cg = Path('/cgroups') / group.lstrip('/')
            # Reject namespace-relative paths escaping this kind node.
            if '..' in cg.parts or not cg.joinpath('memory.stat').exists():
                raise RuntimeError(f'cannot resolve benchmark cgroup {group}')
            data = {'pid': int(path.name), 'cgroup': str(cg)}
            for line in path.joinpath('io').read_text().splitlines():
                k, v = line.split(':')
                data[k] = int(v)
            stat = path.joinpath('stat').read_text().rsplit(')', 1)[1].split()
            data['cpu_seconds'] = (int(stat[11]) + int(stat[12])) / os.sysconf('SC_CLK_TCK')
            data['rss_bytes'] = int(stat[21]) * os.sysconf('SC_PAGE_SIZE')
            data['memory_current'] = int(cg.joinpath('memory.current').read_text())
            data['memory_peak'] = int(cg.joinpath('memory.peak').read_text())
            data['memory_stat'] = dict((k, int(v)) for k, v in (l.split() for l in cg.joinpath('memory.stat').read_text().splitlines()))
            data['cpu_stat'] = dict((k, int(v)) for k, v in (l.split() for l in cg.joinpath('cpu.stat').read_text().splitlines()))
            data['io_stat'] = cg.joinpath('io.stat').read_text()
            result[label] = data
        except (FileNotFoundError, ProcessLookupError):
            continue
    return result


def reclaim():
    before = processes()
    notes = {}
    for label, data in before.items():
        path = Path(data['cgroup']) / 'memory.reclaim'
        try:
            # Request ~1GiB file-page reclaim in this cgroup only. The kernel
            # reports EIO/EAGAIN when it reclaimed what it could below the
            # request; that is the expected best-effort outcome.
            path.write_text('1G')
        except OSError as error:
            notes[label] = f'{errno.errorcode.get(error.errno, error.errno)}: {error.strerror}'
    return {'before': before, 'after': processes(), 'notes': notes}


def start(directory):
    directory.mkdir(parents=True, exist_ok=False)
    recordings = []
    for label, data in processes().items():
        dest = directory / label
        with dest.with_suffix('.log').open('w') as log:
            proc = subprocess.Popen(['perf', 'record', '-e', 'cpu-clock:u', '-F', '19', '--call-graph', 'fp,8192', '-p', str(data['pid']), '-o', str(dest.with_suffix('.data'))], stdout=log, stderr=log, start_new_session=True)
        recordings.append({'label': label, 'perf_pid': proc.pid, 'target_pid': data['pid'], 'start': time.time()})
    time.sleep(0.3)
    for rec in recordings:
        os.kill(rec['perf_pid'], 0)
    (directory / 'recordings.json').write_text(json.dumps(recordings))
    return recordings


def stop(directory):
    recordings = json.loads((directory / 'recordings.json').read_text())
    for rec in recordings:
        try:
            os.kill(rec['perf_pid'], signal.SIGINT)
        except ProcessLookupError:
            rec['stop_note'] = 'perf already exited'
    for rec in recordings:
        for _ in range(100):
            path = Path('/proc') / str(rec['perf_pid']) / 'stat'
            if not path.exists() or path.read_text().rsplit(')', 1)[1].split()[0] == 'Z':
                break
            time.sleep(0.1)
        # Reap the perf child so its output file is complete.
        try:
            os.waitpid(rec['perf_pid'], os.WNOHANG)
        except ChildProcessError:
            pass
        time.sleep(1)
        dest = directory / rec['label']
        # Native libraries belong to the target container's root filesystem.
        # perf is best-effort: record failures instead of failing the run.
        try:
            proc = subprocess.run(['perf', 'script', '-i', str(dest.with_suffix('.data')), '--symfs', f"/proc/{rec['target_pid']}/root"], text=True, capture_output=True, timeout=120)
            dest.with_suffix('.script-log').write_text(proc.stderr)
            if proc.returncode != 0:
                rec['samples'] = 0
                rec['perf_error'] = proc.stderr[-2000:]
                continue
            script = proc.stdout
        except Exception as error:
            rec['samples'] = 0
            rec['perf_error'] = str(error)
            continue
        dest.with_suffix('.stacks').write_text(script)
        counts, stack = collections.Counter(), []
        for line in script.splitlines() + ['']:
            if not line.strip():
                if stack:
                    counts[';'.join(reversed(stack))] += 1
                stack = []
            elif line.startswith('\t'):
                parts = line.strip().split(' ', 1)
                if len(parts) == 2:
                    symbol = parts[1].rsplit(' (', 1)[0].split('+0x', 1)[0].replace(';', ':')
                    stack.append(symbol)
        if not counts:
            rec['samples'] = 0
            rec['perf_note'] = 'no stacks decoded'
            continue
        folded = '\n'.join(f'{stack} {count}' for stack, count in counts.items()) + '\n'
        dest.with_suffix('.folded').write_text(folded)
        try:
            query = urllib.parse.urlencode({'name': f"kind-perf{{service_name=\"{rec['label']}\"}}", 'from': int(rec['start']), 'until': int(time.time()), 'format': 'folded', 'sampleRate': 19, 'units': 'samples', 'aggregationType': 'sum'})
            req = urllib.request.Request('http://lgtm.monitoring:4040/ingest?' + query, data=folded.encode(), headers={'Content-Type': 'text/plain'})
            with urllib.request.urlopen(req, timeout=30) as response:
                rec['ingest_status'] = response.status
        except Exception as error:
            rec['ingest_error'] = str(error)
        rec['samples'] = sum(counts.values())
    (directory / 'recordings.json').write_text(json.dumps(recordings, indent=2))
    return recordings


if __name__ == '__main__':
    action = sys.argv[1]
    if action == 'stats':
        result = processes()
    elif action == 'reclaim':
        result = reclaim()
    elif action == 'start':
        result = start(Path(sys.argv[2]))
    elif action == 'stop':
        result = stop(Path(sys.argv[2]))
    else:
        raise SystemExit('unknown action')
    print(json.dumps(result))
