import requests
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from clickhouse_driver import Client
from datetime import datetime, timezone
import time
import json
import logging
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TwitchLanguageAnalyzer:
    def __init__(self, client_id: str, client_secret: str, clickhouse_config: Dict):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token: Optional[str] = None
        self.clickhouse_client = Client(**clickhouse_config)
        
        self._init_database()
        
    def _get_access_token(self) -> None:
        url = "https://id.twitch.tv/oauth2/token"
        params = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'grant_type': 'client_credentials'
        }
        
        try:
            response = requests.post(url, params=params)
            response.raise_for_status()
            self.access_token = response.json()['access_token']
            logger.info("Access token получен успешно")
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка получения token: {e}")
            raise
    
    def _make_twitch_request(self, url: str, params: Optional[Dict] = None) -> Dict:
        if self.access_token is None:
            self._get_access_token()
            
        headers = {
            'Client-ID': self.client_id,
            'Authorization': f'Bearer {self.access_token}'
        }
        
        try:
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                logger.info("Token истек, получаем новый...")
                self._get_access_token()
                headers['Authorization'] = f'Bearer {self.access_token}'
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                return response.json()
            else:
                response.raise_for_status()
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка API запроса: {e}")
            raise
    
    def _parse_twitch_datetime(self, date_string: str) -> datetime:
        try:
            if date_string.endswith('Z'):
                date_string = date_string[:-1] + '+00:00'
            return datetime.fromisoformat(date_string)
        except (ValueError, AttributeError) as e:
            logger.warning(f"Ошибка парсинга даты {date_string}: {e}. Использую текущее время.")
            return datetime.now(timezone.utc)
    
    def _init_database(self) -> None:
        """Инициализация таблиц в ClickHouse"""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS twitch_streams (
            stream_id String,
            user_id String,
            user_name String,
            game_id String,
            game_name String,
            title String,
            viewer_count Int32,
            started_at DateTime64,
            language String,
            thumbnail_url String,
            tags Array(String),
            is_mature Boolean,
            collected_at DateTime64 DEFAULT now()
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(collected_at)
        ORDER BY (collected_at, game_name, language)
        """
        
        try:
            self.clickhouse_client.execute(create_table_query)
            logger.info("Таблица twitch_streams инициализирована")
        except Exception as e:
            logger.error(f"Ошибка инициализации базы данных: {e}")
            raise
    
    def get_top_streams(self, limit: int = 100) -> List[Dict]:
        """Получение топовых стримов с Twitch"""
        url = "https://api.twitch.tv/helix/streams"
        params = {
            'first': limit
        }
        
        try:
            data = self._make_twitch_request(url, params)
            return data.get('data', [])
        except Exception as e:
            logger.error(f"Ошибка получения стримов: {e}")
            return []
    
    def get_game_details(self, game_ids: List[str]) -> Dict[str, str]:
        """Получение информации об играх"""
        if not game_ids:
            return {}
            
        url = "https://api.twitch.tv/helix/games"
        game_info = {}
        
        try:
            for i in range(0, len(game_ids), 100):
                batch = game_ids[i:i+100]
                params = {'id': batch}
                data = self._make_twitch_request(url, params)
                
                for game in data.get('data', []):
                    game_info[game['id']] = game['name']
            
            return game_info
        except Exception as e:
            logger.error(f"Ошибка получения информации об играх: {e}")
            return {}
    
    def collect_and_store_data(self) -> bool:
        """Сбор данных и сохранение в ClickHouse"""
        logger.info("Сбор данных с Twitch API...")
        
        try:
            streams = self.get_top_streams(100)
            
            if not streams:
                logger.warning("Не удалось получить данные стримов")
                return False
            
            game_ids = list(set(stream['game_id'] for stream in streams if stream['game_id']))
            game_names = self.get_game_details(game_ids)
            
            insert_data = []
            for stream in streams:
                try:
                    started_at = self._parse_twitch_datetime(stream['started_at'])
                    
                    insert_data.append({
                        'stream_id': stream['id'],
                        'user_id': stream['user_id'],
                        'user_name': stream['user_name'],
                        'game_id': stream['game_id'] or '',
                        'game_name': game_names.get(stream['game_id'], 'Unknown'),
                        'title': stream['title'][:500] if stream['title'] else '',
                        'viewer_count': stream['viewer_count'],
                        'started_at': started_at,
                        'language': stream['language'] or 'unknown',
                        'thumbnail_url': stream['thumbnail_url'][:500] if stream['thumbnail_url'] else '',
                        'tags': stream.get('tag_ids', []) or [],
                        'is_mature': stream.get('is_mature', False)
                    })
                except Exception as e:
                    logger.warning(f"Ошибка обработки стрима {stream.get('id', 'unknown')}: {e}")
                    continue
            
            if not insert_data:
                logger.warning("Нет данных для вставки")
                return False
            
            insert_query = """
            INSERT INTO twitch_streams 
            (stream_id, user_id, user_name, game_id, game_name, title, viewer_count, started_at, language, thumbnail_url, tags, is_mature)
            VALUES
            """
            
            self.clickhouse_client.execute(insert_query, insert_data)
            logger.info(f"Успешно сохранено {len(insert_data)} стримов в ClickHouse")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при сборе и сохранении данных: {e}")
            return False
    
    def analyze_language_popularity(self) -> pd.DataFrame:
        """Анализ популярности языков"""
        query = """
        SELECT 
            language,
            COUNT(*) as stream_count,
            SUM(viewer_count) as total_viewers,
            AVG(viewer_count) as avg_viewers,
            COUNT(DISTINCT user_id) as unique_streamers
        FROM twitch_streams 
        WHERE collected_at >= now() - INTERVAL 1 DAY
        GROUP BY language
        HAVING stream_count > 1 AND language != 'unknown'
        ORDER BY total_viewers DESC
        """
        
        try:
            result = self.clickhouse_client.execute(query)
            
            if not result:
                logger.warning("Нет данных для анализа")
                return pd.DataFrame()
            
            df = pd.DataFrame(result, columns=['language', 'stream_count', 'total_viewers', 'avg_viewers', 'unique_streamers'])
            return df
            
        except Exception as e:
            logger.error(f"Ошибка анализа популярности языков: {e}")
            return pd.DataFrame()
    
    def classify_languages(self, df: pd.DataFrame) -> pd.DataFrame:
        """Классификация языков по популярности"""
        if df.empty:
            return df
        
        try:
            if len(df) >= 4:
                quantiles = df['total_viewers'].quantile([0.2, 0.5, 0.8])
                q20, q50, q80 = quantiles[0.2], quantiles[0.5], quantiles[0.8]
                
                conditions = [
                    df['total_viewers'] >= q80,
                    df['total_viewers'] >= q50,
                    df['total_viewers'] >= q20,
                    df['total_viewers'] < q20
                ]
                
                df['popularity_category'] = np.select(
                    conditions, 
                    ['Очень высокая', 'Высокая', 'Средняя', 'Низкая'], 
                    default='Средняя'
                )
            else:
                df['popularity_category'] = 'Средняя'
            
            return df
            
        except Exception as e:
            logger.error(f"Ошибка классификации языков: {e}")
            df['popularity_category'] = 'Неизвестно'
            return df
    
    def create_visualizations(self, df: pd.DataFrame) -> None:
        """Создание визуализаций"""
        if df.empty:
            logger.warning("Нет данных для визуализации")
            return
        
        try:
            plt.style.use('default')
            sns.set_palette("husl")
            
            fig, axes = plt.subplots(2, 2, figsize=(15, 12))
            fig.suptitle('Анализ популярности языков на Twitch', fontsize=16, fontweight='bold')
            
            top_languages = df.nlargest(10, 'total_viewers')
            if not top_languages.empty:
                axes[0, 0].bar(top_languages['language'], top_languages['total_viewers'])
                axes[0, 0].set_title('Топ языков по количеству зрителей')
                axes[0, 0].set_ylabel('Общее количество зрителей')
                axes[0, 0].tick_params(axis='x', rotation=45)
            
            if not top_languages.empty:
                axes[0, 1].pie(top_languages['stream_count'], labels=top_languages['language'], autopct='%1.1f%%')
                axes[0, 1].set_title('Распределение стримов по языкам')
            
            if not top_languages.empty:
                axes[1, 0].bar(top_languages['language'], top_languages['avg_viewers'])
                axes[1, 0].set_title('Среднее количество зрителей по языкам')
                axes[1, 0].set_ylabel('Среднее количество зрителей')
                axes[1, 0].tick_params(axis='x', rotation=45)
            
            popularity_counts = df['popularity_category'].value_counts()
            if not popularity_counts.empty:
                axes[1, 1].pie(popularity_counts.values, labels=popularity_counts.index, autopct='%1.1f%%')
                axes[1, 1].set_title('Классификация языков по популярности')
            
            plt.tight_layout()
            plt.savefig('twitch_language_analysis.png', dpi=300, bbox_inches='tight')
            plt.show()
            
            numeric_columns = ['stream_count', 'total_viewers', 'avg_viewers', 'unique_streamers']
            numeric_df = df[numeric_columns] if all(col in df.columns for col in numeric_columns) else pd.DataFrame()
            
            if not numeric_df.empty and len(numeric_df) > 1:
                plt.figure(figsize=(8, 6))
                correlation_matrix = numeric_df.corr()
                sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0)
                plt.title('Корреляция метрик популярности')
                plt.tight_layout()
                plt.savefig('twitch_correlation_heatmap.png', dpi=300, bbox_inches='tight')
                plt.show()
                
        except Exception as e:
            logger.error(f"Ошибка создания визуализаций: {e}")
    
    def run_analysis(self) -> None:
        """Запуск полного анализа"""
        try:
            success = self.collect_and_store_data()
            
            if not success:
                logger.error("Не удалось собрать данные")
                return
            

            df = self.analyze_language_popularity()
            
            if not df.empty:
                df = self.classify_languages(df)
                
                print("\n" + "="*50)
                print("Топ языков по популярности:")
                print("="*50)
                print(df[['language', 'total_viewers', 'stream_count', 'popularity_category']].head(10))
                
                self.create_visualizations(df)
                
                df.to_csv('twitch_language_analysis.csv', index=False, encoding='utf-8')
                logger.info("Результаты сохранены в twitch_language_analysis.csv")
            else:
                logger.warning("Недостаточно данных для анализа")
                
        except Exception as e:
            logger.error(f"Ошибка при выполнении анализа: {e}")

def main():
    config = {
        'client_id': '', 
        'client_secret': ']',
        'clickhouse_config': {
            'host': 'localhost',
            'port': 9000,
            'user': 'default',
            'password': '123',
            'database': 'default'
        }
    }
    
    if config['client_id'] == 'YOUR_TWITCH_CLIENT_ID' or config['client_secret'] == 'YOUR_TWITCH_CLIENT_SECRET':
        print("Пожалуйста, установите ваши Twitch API credentials в конфигурации")
        return
    
    try:
        analyzer = TwitchLanguageAnalyzer(
            client_id=config['client_id'],
            client_secret=config['client_secret'],
            clickhouse_config=config['clickhouse_config']
        )
        
        analyzer.run_analysis()
        
    except Exception as e:
        logger.error(f"Ошибка запуска приложения: {e}")

if __name__ == "__main__":
    try:
        import numpy as np
    except ImportError:
        print("Установите numpy: pip install numpy")
        exit(1)
    
    main()