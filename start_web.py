#!/usr/bin/env python3
"""
TradingAgents-CN 简化启动脚本
解决模块导入问题的最简单方案
"""

import os
import sys
import subprocess
from pathlib import Path

def main():
    """主函数"""
    print("🚀 TradingAgents-CN Web应用启动器")
    print("=" * 50)
    
    # 获取项目根目录
    project_root = Path(__file__).parent
    web_dir = project_root / "web"
    app_file = web_dir / "app.py"
    
    # 检查文件是否存在
    if not app_file.exists():
        print(f"❌ 找不到应用文件: {app_file}")
        return
    
    # 检查虚拟环境
    in_venv = (
        hasattr(sys, 'real_prefix') or 
        (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)
    )
    
    if not in_venv:
        print("⚠️ 建议在虚拟环境中运行:")
        print("   Windows: .\\env\\Scripts\\activate")
        print("   Linux/macOS: source env/bin/activate")
        print()
    
    # 检查并安装首屏依赖
    required_pkgs = [
        "streamlit",
        "plotly",
        "pandas",
        "numpy",
        "python-dotenv"
    ]
    missing = []
    for mod, pkg in [
        ("streamlit", "streamlit"),
        ("plotly", "plotly"),
        ("pandas", "pandas"),
        ("numpy", "numpy"),
        ("dotenv", "python-dotenv")
    ]:
        try:
            __import__(mod)
        except ImportError:
            missing.append(pkg)
    if missing:
        print(f"❌ 缺少依赖，正在安装: {', '.join(missing)}")
        try:
            subprocess.run([sys.executable, "-m", "pip", "install", "-U", *missing], check=True)
            print("✅ 首屏依赖安装完成")
        except subprocess.CalledProcessError:
            print("❌ 依赖安装失败，请手动安装: pip install -U " + ' '.join(missing))
            return
    else:
        print("✅ 首屏依赖已满足")
    
    # 设置环境变量，添加项目根目录到Python路径
    env = os.environ.copy()
    current_path = env.get('PYTHONPATH', '')
    if current_path:
        env['PYTHONPATH'] = f"{project_root}{os.pathsep}{current_path}"
    else:
        env['PYTHONPATH'] = str(project_root)
    
    # 构建启动命令
    cmd = [
        sys.executable, "-m", "streamlit", "run",
        str(app_file),
        "--server.port", "8501",
        "--server.address", "0.0.0.0",
        "--browser.gatherUsageStats", "false",
        "--server.fileWatcherType", "none",
        "--server.runOnSave", "false"
    ]
    
    print("🌐 启动Web应用...")
    print("📱 浏览器将自动打开 http://0.0.0.0:8501")
    print("⏹️  按 Ctrl+C 停止应用")
    print("=" * 50)
    
    try:
        # 启动应用，传递修改后的环境变量
        subprocess.run(cmd, cwd=project_root, env=env)
    except KeyboardInterrupt:
        print("\n⏹️ Web应用已停止")
    except Exception as e:
        print(f"\n❌ 启动失败: {e}")
        print("\n💡 如果遇到模块导入问题，请尝试:")
        print("   1. 激活虚拟环境")
        print("   2. 运行: pip install -e .")
        print("   3. 再次启动Web应用")

if __name__ == "__main__":
    main()
