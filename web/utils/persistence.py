"""
持久化工具
使用URL参数和session state结合的方式来持久化用户选择
"""

import streamlit as st
import logging
from urllib.parse import urlencode, parse_qs
import json

logger = logging.getLogger(__name__)

class ModelPersistence:
    """模型选择持久化管理器"""
    
    def __init__(self):
        self.storage_key = "model_config"
    
    def save_config(self, provider, category, model):
        """保存配置到session state和URL"""
        config = {
            'provider': provider,
            'category': category,
            'model': model
        }
        
        # 保存到session state
        st.session_state[self.storage_key] = config
        
        # 保存到URL参数（通过query_params）
        try:
            st.query_params.update({
                'provider': provider,
                'category': category,
                'model': model
            })
            logger.debug(f"💾 [Persistence] 配置已保存: {config}")
        except Exception as e:
            logger.warning(f"⚠️ [Persistence] URL参数保存失败: {e}")
    
    def load_config(self):
        """Purpose: Load model selection from URL or session state.

        Remarks:
        - Falls back to DeepSeek provider when no prior config is present.
        - Syncs with Streamlit `st.session_state` and `st.query_params`.
        - Avoids raising; provides safe defaults for first-run UX.

        Returns:
        - A dict containing `provider`, `category`, and `model` fields.

        Example:
        - cfg = persistence.load_config()  // returns defaults if unset
        """
        # 首先尝试从URL参数加载
        try:
            query_params = st.query_params
            if 'provider' in query_params:
                config = {
                    'provider': query_params.get('provider', 'deepseek'),
                    'category': query_params.get('category', 'openai'),
                    'model': query_params.get('model', '')
                }
                logger.debug(f"📥 [Persistence] 从URL加载配置: {config}")
                return config
        except Exception as e:
            logger.warning(f"⚠️ [Persistence] URL参数加载失败: {e}")
        
        # 然后尝试从session state加载
        if self.storage_key in st.session_state:
            config = st.session_state[self.storage_key]
            logger.debug(f"📥 [Persistence] 从Session State加载配置: {config}")
            return config
        
        # 返回默认配置
        default_config = {
            'provider': 'deepseek',
            'category': 'openai',
            'model': ''
        }
        logger.debug(f"📥 [Persistence] 使用默认配置: {default_config}")
        return default_config
    
    def clear_config(self):
        """清除配置"""
        if self.storage_key in st.session_state:
            del st.session_state[self.storage_key]
        
        try:
            st.query_params.clear()
            logger.info("🗑️ [Persistence] 配置已清除")
        except Exception as e:
            logger.warning(f"⚠️ [Persistence] 清除失败: {e}")

# 全局实例
persistence = ModelPersistence()

def save_model_selection(provider, category="", model=""):
    """保存模型选择"""
    persistence.save_config(provider, category, model)

def load_model_selection():
    """加载模型选择"""
    return persistence.load_config()

def clear_model_selection():
    """清除模型选择"""
    persistence.clear_config()
