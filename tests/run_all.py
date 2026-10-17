#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""跑全部单元测试（CI 与本地通用）。

用法: python tests/run_all.py
退出码: 0 全部通过, 1 有失败。
"""
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SUITES = [
    "test_watchdog.py",
    "test_quality_routing.py",
    "test_account_affinity.py",
    "test_endpoints.py",
]


def main():
    failed = []
    for name in SUITES:
        path = os.path.join(HERE, name)
        if not os.path.exists(path):
            print("[FAIL] 缺失测试文件: %s" % name)
            failed.append(name)
            continue
        print("=" * 70)
        print("[RUN ] %s" % name)
        r = subprocess.run([sys.executable, path], cwd=ROOT,
                           capture_output=True, text=True)
        tail = (r.stdout or "") + (r.stderr or "")
        print(tail[-4000:])
        if r.returncode == 0:
            print("[PASS] %s" % name)
        else:
            print("[FAIL] %s (exit=%s)" % (name, r.returncode))
            failed.append(name)
    print("=" * 70)
    if failed:
        print("结果: FAIL %d/%d -> %s" % (len(failed), len(SUITES), ", ".join(failed)))
        return 1
    print("结果: ALL PASS (%d 个套件)" % len(SUITES))
    return 0


if __name__ == "__main__":
    sys.exit(main())