#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
构建 Windows 单文件可执行版 ModelRotator。

用法:
    python build_exe.py             # 构建 dist/ModelRotator.exe
    python build_exe.py --clean     # 先清理 build/ 与 dist/

依赖: PyInstaller (pip install pyinstaller)
产物: dist/ModelRotator.exe  (单文件, 双击即用, 配置随 exe 同目录)
"""

import argparse
import os
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
APP_NAME = "ModelRotator"
ENTRY = os.path.join(HERE, "rotator.py")
EXTRA = ["config.example.json"]          # 随 exe 捆绑, 首次运行时用于引导


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--clean", action="store_true", help="先删除 build/ 与 dist/")
    ap.add_argument("--console", action="store_true",
                    help="保留控制台窗口（便于看日志）；默认 --windowed 隐藏")
    args = ap.parse_args()

    if args.clean:
        for d in ("build", "dist"):
            p = os.path.join(HERE, d)
            if os.path.isdir(p):
                shutil.rmtree(p)
                print("[build] 已清理 %s" % p)

    # PyInstaller 参数。--onefile 打单文件; --noconsole 隐藏黑窗(默认)。
    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--name", APP_NAME,
        "--onefile",
        "--distpath", os.path.join(HERE, "dist"),
        "--workpath", os.path.join(HERE, "build"),
        "--specpath", os.path.join(HERE, "build"),
        "--clean",
        "--noupx",
    ]
    if not args.console:
        cmd.append("--noconsole")

    for f in EXTRA:
        p = os.path.join(HERE, f)
        if os.path.exists(p):
            cmd += ["--add-data", "%s%s." % (p, os.pathsep)]
        else:
            print("[build] 警告: 缺少 %s, exe 内不含该文件" % f)

    cmd.append(ENTRY)

    print("[build] 执行: %s" % " ".join(cmd))
    r = subprocess.run(cmd)
    if r.returncode != 0:
        print("[build] PyInstaller 失败, 退出码 %s" % r.returncode)
        sys.exit(r.returncode)

    exe = os.path.join(HERE, "dist", APP_NAME + ".exe")
    if os.path.exists(exe):
        print("[build] 完成: %s (%d bytes)" % (exe, os.path.getsize(exe)))
        print("[build] 分发时把 exe 与 config.example.json 一起给接收方；")
        print("[build] 首次运行会自动在 exe 同目录生成 config.json，编辑后生效。")
    else:
        print("[build] 未找到产物, 可能构建失败")
        sys.exit(1)


if __name__ == "__main__":
    main()