#!/usr/bin/env python3
"""
Docker环境PDF导出适配器
处理Docker容器中的PDF生成特殊需求
"""

import os
import subprocess
import tempfile
from typing import Optional

# 导入日志模块
from tradingagents.utils.logging_manager import get_logger
logger = get_logger('web')

def is_docker_environment() -> bool:
    """检测是否在Docker环境中运行"""
    try:
        # 检查/.dockerenv文件
        if os.path.exists('/.dockerenv'):
            return True
        
        # 检查cgroup信息
        with open('/proc/1/cgroup', 'r') as f:
            content = f.read()
            if 'docker' in content or 'containerd' in content:
                return True
    except:
        pass
    
    # 检查环境变量
    return os.environ.get('DOCKER_CONTAINER', '').lower() == 'true'

def setup_xvfb_display():
    """设置虚拟显示器 (Docker环境需要)"""
    if not is_docker_environment():
        return True

    try:
        # 检查Xvfb是否已经在运行
        try:
            result = subprocess.run(['pgrep', 'Xvfb'], capture_output=True, timeout=2)
            if result.returncode == 0:
                logger.info(f"✅ Xvfb已在运行")
                os.environ['DISPLAY'] = ':99'
                return True
        except:
            pass

        # 启动Xvfb虚拟显示器 (后台运行)
        subprocess.Popen([
            'Xvfb', ':99', '-screen', '0', '1024x768x24', '-ac', '+extension', 'GLX'
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # 等待一下让Xvfb启动
        import time
        time.sleep(2)

        # 设置DISPLAY环境变量
        os.environ['DISPLAY'] = ':99'
        logger.info(f"✅ Docker虚拟显示器设置成功")
        return True
    except Exception as e:
        logger.error(f"⚠️ 虚拟显示器设置失败: {e}")
        # 即使Xvfb失败，也尝试继续，某些情况下wkhtmltopdf可以无头运行
        return False

def get_docker_wkhtmltopdf_args():
    """获取Docker环境下wkhtmltopdf的特殊参数"""
    if not is_docker_environment():
        return []

    # 这些是wkhtmltopdf的参数，不是pandoc的参数
    return [
        '--disable-smart-shrinking',
        '--print-media-type',
        '--no-background',
        '--disable-javascript',
        '--quiet'
    ]

def _binary_ok(cmd: list[str]) -> bool:
    """检查命令是否可用并返回0退出码"""
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=10)
        return result.returncode == 0
    except Exception:
        return False

def _available_engine() -> str:
    """选择可用的PDF引擎，优先wkhtmltopdf，其次weasyprint，最后default"""
    if _binary_ok(['wkhtmltopdf', '--version']):
        return 'wkhtmltopdf'
    if _binary_ok(['weasyprint', '--version']):
        return 'weasyprint'
    return 'default'

def test_docker_pdf_generation() -> bool:
    """测试Docker环境下的PDF生成（根据可用引擎选择）"""
    if not is_docker_environment():
        return True

    try:
        import pypandoc

        # 测试内容
        test_html = """
        <html>
        <head>
            <meta charset="UTF-8">
            <title>Docker PDF Test</title>
        </head>
        <body>
            <h1>Docker PDF 测试</h1>
            <p>这是在Docker环境中生成的PDF测试文档。</p>
            <p>中文字符测试：你好世界！</p>
        </body>
        </html>
        """

        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
            output_file = tmp.name

        engine = _available_engine()
        extra_args = []

        if engine == 'wkhtmltopdf':
            # wkhtmltopdf 需要虚拟显示器
            setup_xvfb_display()
            extra_args = [
                '--pdf-engine=wkhtmltopdf',
                '--pdf-engine-opt=--disable-smart-shrinking',
                '--pdf-engine-opt=--quiet'
            ]
        elif engine == 'weasyprint':
            extra_args = ['--pdf-engine=weasyprint']

        pypandoc.convert_text(
            test_html,
            'pdf',
            format='html',
            outputfile=output_file,
            extra_args=extra_args
        )

        # 检查文件是否生成
        if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
            os.unlink(output_file)  # 清理测试文件
            logger.info(f"✅ Docker PDF生成测试成功（引擎: {engine}）")
            return True
        else:
            logger.error(f"❌ Docker PDF生成测试失败（引擎: {engine}）")
            return False

    except Exception as e:
        logger.error(f"❌ Docker PDF测试失败: {e}")
        return False

def get_docker_pdf_extra_args():
    """获取Docker环境下PDF生成的额外参数"""
    base_args = [
        '--toc',
        '--number-sections',
        '-V', 'geometry:margin=2cm',
        '-V', 'documentclass=article'
    ]

    if is_docker_environment():
        # Docker环境下的特殊配置 - 使用正确的pandoc参数格式
        docker_args = []
        wkhtmltopdf_args = get_docker_wkhtmltopdf_args()

        # 将wkhtmltopdf参数正确传递给pandoc
        for arg in wkhtmltopdf_args:
            docker_args.extend(['--pdf-engine-opt=' + arg])

        return base_args + docker_args

    return base_args

def check_docker_pdf_dependencies():
    """检查Docker环境下PDF生成的依赖，支持weasyprint回退"""
    if not is_docker_environment():
        return True, "非Docker环境"

    wkhtmltopdf_ok = _binary_ok(['wkhtmltopdf', '--version'])
    weasyprint_ok = _binary_ok(['weasyprint', '--version'])

    # 字体检查（两种引擎都需要）
    font_paths = [
        '/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc',
        '/usr/share/fonts/truetype/wqy/wqy-microhei.ttc',
        '/usr/share/fonts/truetype/liberation/',
        # 项目本地字体挂载位置（在docker-compose中挂载）
        '/usr/share/fonts/tradingagents'
    ]
    font_found = any(os.path.exists(path) for path in font_paths)

    # 进一步检测是否存在苹方或SF Pro类字体文件
    if not font_found:
        try:
            candidates = []
            for base in ['/usr/share/fonts/tradingagents']:
                if os.path.isdir(base):
                    for name in os.listdir(base):
                        if name.lower().endswith(('.ttf', '.ttc', '.otf')):
                            candidates.append(os.path.join(base, name))
            font_found = len(candidates) > 0
        except Exception:
            pass

    # 对wkhtmltopdf而言，Xvfb是推荐的
    xvfb_ok = _binary_ok(['Xvfb', '-help'])

    # 优先级：wkhtmltopdf -> weasyprint -> default
    if wkhtmltopdf_ok:
        missing = []
        if not xvfb_ok:
            missing.append('xvfb')
        if not font_found:
            missing.append('chinese-fonts')
        if missing:
            return False, f"缺少依赖: {', '.join(missing)}"
        return True, "wkhtmltopdf与字体可用"

    if weasyprint_ok:
        # 使用weasyprint时无需Xvfb
        if not font_found:
            return False, "缺少依赖: 字体(建议挂载macOS字体或安装Noto CJK)"
        return True, "wkhtmltopdf缺失，但已检测到weasyprint，将使用weasyprint"

    # 两者都不可用
    return False, "缺少依赖: wkhtmltopdf, weasyprint"

def get_docker_status_info():
    """获取Docker环境状态信息"""
    info = {
        'is_docker': is_docker_environment(),
        'dependencies_ok': False,
        'dependency_message': '',
        'pdf_test_ok': False
    }
    
    if info['is_docker']:
        info['dependencies_ok'], info['dependency_message'] = check_docker_pdf_dependencies()
        if info['dependencies_ok']:
            info['pdf_test_ok'] = test_docker_pdf_generation()
    else:
        info['dependencies_ok'] = True
        info['dependency_message'] = '非Docker环境，使用标准配置'
        info['pdf_test_ok'] = True
    
    return info

if __name__ == "__main__":
    logger.info(f"🐳 Docker PDF适配器测试")
    logger.info(f"=")
    
    status = get_docker_status_info()
    
    logger.info(f"Docker环境: {'是' if status['is_docker'] else '否'}")
    logger.error(f"依赖检查: {'✅' if status['dependencies_ok'] else '❌'} {status['dependency_message']}")
    logger.error(f"PDF测试: {'✅' if status['pdf_test_ok'] else '❌'}")
    
    if status['is_docker'] and status['dependencies_ok'] and status['pdf_test_ok']:
        logger.info(f"\n🎉 Docker PDF功能完全正常！")
    elif status['is_docker'] and not status['dependencies_ok']:
        logger.warning(f"\n⚠️ Docker环境缺少PDF依赖，请重新构建镜像")
    elif status['is_docker'] and not status['pdf_test_ok']:
        logger.error(f"\n⚠️ Docker PDF测试失败，可能需要调整配置")
    else:
        logger.info(f"\n✅ 非Docker环境，使用标准PDF配置")
