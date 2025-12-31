"""
CLAVIS DATA ENGINEER CHALLENGE
Utilitários de Gerenciamento de Dados

Gerencia persistência de dados, validação e gerenciamento de arquivos.
"""

import os
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import hashlib


class DataManager:
    """
    Gerencia persistência de dados e operações de arquivo.

    Recursos:
    - Gerenciamento de arquivos JSON com gravações atômicas
    - Validação de dados
    - Detecção de duplicados
    - Organização de arquivos por data
    """

    def __init__(self, base_path: str = "/opt/airflow/data"):
        """
        Inicializa o DataManager.

        Args:
            base_path: Diretório base para armazenamento de dados
        """
        self.base_path = Path(base_path)
        self.raw_path = self.base_path / "raw"
        self.processed_path = self.base_path / "processed"
        self.logger = logging.getLogger(__name__)

        # Cria diretórios
        self.raw_path.mkdir(parents=True, exist_ok=True)
        self.processed_path.mkdir(parents=True, exist_ok=True)

    def save_json(
        self,
        data: Any,
        filename: str,
        output_dir: Optional[Path] = None,
        pretty: bool = True
    ) -> Path:
        """
        Salva dados em arquivo JSON com gravação atômica.

        Args:
            data: Dados a serem salvos
            filename: Nome do arquivo de saída
            output_dir: Diretório de saída (padrão: raw_path)
            pretty: Formatar JSON com indentação

        Returns:
            Caminho para o arquivo salvo
        """
        output_dir = output_dir or self.raw_path
        output_dir.mkdir(parents=True, exist_ok=True)

        file_path = output_dir / filename

        # Gravação atômica: escreve em arquivo temporário, depois renomeia
        temp_path = file_path.with_suffix('.tmp')

        try:
            with open(temp_path, 'w', encoding='utf-8') as f:
                if pretty:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                else:
                    json.dump(data, f, ensure_ascii=False)

            # Renomeação atômica
            temp_path.replace(file_path)

            self.logger.info(f"Successfully saved data to {file_path}")
            return file_path

        except Exception as e:
            self.logger.error(f"Failed to save data to {file_path}: {e}")
            # Limpa arquivo temporário
            if temp_path.exists():
                temp_path.unlink()
            raise

    def save_ndjson(
        self,
        data: List[Dict[str, Any]],
        filename: str,
        output_dir: Optional[Path] = None
    ) -> Path:
        """
        Salva dados em formato NDJSON (Newline Delimited JSON).
        Cada linha é um objeto JSON completo - perfeito para processamento streaming/ELK.

        Exemplo de formato:
        {"timestamp": "2025-12-31T10:30:15Z", "symbol": "BTC", "price": 50000}
        {"timestamp": "2025-12-31T10:30:15Z", "symbol": "ETH", "price": 3000}

        Args:
            data: Lista de dicionários para salvar (um JSON por linha)
            filename: Nome do arquivo de saída
            output_dir: Diretório de saída (padrão: raw_path)

        Returns:
            Caminho para o arquivo salvo
        """
        output_dir = output_dir or self.raw_path
        output_dir.mkdir(parents=True, exist_ok=True)

        file_path = output_dir / filename

        # Gravação atômica: escreve em arquivo temporário, depois renomeia
        temp_path = file_path.with_suffix('.tmp')

        try:
            with open(temp_path, 'w', encoding='utf-8') as f:
                for item in data:
                    # Escreve cada item como uma única linha JSON (compacto, sem indentação)
                    json.dump(item, f, ensure_ascii=False)
                    f.write('\n')

            # Renomeação atômica
            temp_path.replace(file_path)

            self.logger.info(f"Successfully saved NDJSON data to {file_path} ({len(data)} records)")
            return file_path

        except Exception as e:
            self.logger.error(f"Failed to save NDJSON to {file_path}: {e}")
            # Limpa arquivo temporário
            if temp_path.exists():
                temp_path.unlink()
            raise

    def save_weather_data(
        self,
        weather_data: List[Dict[str, Any]],
        timestamp: Optional[datetime] = None
    ) -> Path:
        """
        Salva dados meteorológicos com timestamp.

        Args:
            weather_data: Lista de registros meteorológicos
            timestamp: Timestamp para nome do arquivo (padrão: agora)

        Returns:
            Caminho para o arquivo salvo
        """
        timestamp = timestamp or datetime.utcnow()
        filename = f"weather_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"

        return self.save_json(weather_data, filename)

    def save_air_quality_data(
        self,
        air_quality_data: List[Dict[str, Any]],
        timestamp: Optional[datetime] = None
    ) -> Path:
        """
        Salva dados de qualidade do ar com timestamp.

        Args:
            air_quality_data: Lista de registros de qualidade do ar
            timestamp: Timestamp para nome do arquivo (padrão: agora)

        Returns:
            Caminho para o arquivo salvo
        """
        timestamp = timestamp or datetime.utcnow()
        filename = f"air_quality_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"

        return self.save_json(air_quality_data, filename)

    def save_forecast_data(
        self,
        forecast_data: List[Dict[str, Any]],
        timestamp: Optional[datetime] = None
    ) -> Path:
        """
        Salva dados de previsão com timestamp.

        Args:
            forecast_data: Lista de registros de previsão
            timestamp: Timestamp para nome do arquivo (padrão: agora)

        Returns:
            Caminho para o arquivo salvo
        """
        timestamp = timestamp or datetime.utcnow()
        filename = f"forecast_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"

        return self.save_json(forecast_data, filename)

    def save_pipeline_execution(
        self,
        execution_data: Dict[str, Any],
        timestamp: Optional[datetime] = None
    ) -> Path:
        """
        Salva métricas de execução do pipeline em formato NDJSON.

        Args:
            execution_data: Métricas de execução
            timestamp: Timestamp para nome do arquivo (padrão: agora)

        Returns:
            Caminho para o arquivo salvo
        """
        timestamp = timestamp or datetime.utcnow()
        filename = f"pipeline_execution_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"

        # Salva como NDJSON (JSON de linha única para consistência com outros tipos de dados)
        return self.save_ndjson([execution_data], filename)

    def validate_weather_data(self, data: Dict[str, Any]) -> bool:
        """
        Valida estrutura de dados meteorológicos.

        Args:
            data: Dicionário de dados meteorológicos

        Returns:
            True se válido, False caso contrário
        """
        required_fields = ['name', 'main', 'weather', 'coord']

        try:
            # Verifica campos obrigatórios de nível superior
            for field in required_fields:
                if field not in data:
                    self.logger.warning(f"Missing required field: {field}")
                    return False

            # Verifica campos obrigatórios aninhados
            if 'temp' not in data.get('main', {}):
                self.logger.warning("Missing 'temp' in 'main'")
                return False

            if 'lat' not in data.get('coord', {}) or 'lon' not in data.get('coord', {}):
                self.logger.warning("Missing coordinates")
                return False

            return True

        except Exception as e:
            self.logger.error(f"Error validating weather data: {e}")
            return False

    def validate_air_quality_data(self, data: Dict[str, Any]) -> bool:
        """
        Valida estrutura de dados de qualidade do ar.

        Args:
            data: Dicionário de dados de qualidade do ar

        Returns:
            True se válido, False caso contrário
        """
        try:
            # Verifica se a lista existe
            if 'list' not in data or not data['list']:
                self.logger.warning("Missing or empty 'list' field")
                return False

            # Verifica primeiro item na lista
            first_item = data['list'][0]

            # Verifica dados principais de AQI
            if 'main' not in first_item or 'aqi' not in first_item['main']:
                self.logger.warning("Missing AQI data")
                return False

            # Verifica componentes
            if 'components' not in first_item:
                self.logger.warning("Missing components data")
                return False

            return True

        except Exception as e:
            self.logger.error(f"Error validating air quality data: {e}")
            return False

    def calculate_data_hash(self, data: Dict[str, Any]) -> str:
        """
        Calcula hash dos dados para detecção de duplicados.

        Args:
            data: Dicionário de dados

        Returns:
            String de hash MD5
        """
        # Ordena chaves para hash consistente
        data_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(data_str.encode('utf-8')).hexdigest()

    def list_files(
        self,
        pattern: str = "*",
        directory: Optional[Path] = None
    ) -> List[Path]:
        """
        Lista arquivos que correspondem ao padrão.

        Args:
            pattern: Padrão glob (ex: 'weather_*.json')
            directory: Diretório para buscar (padrão: raw_path)

        Returns:
            Lista de caminhos de arquivos correspondentes
        """
        directory = directory or self.raw_path
        return sorted(directory.glob(pattern))

    def get_file_stats(self, file_path: Path) -> Dict[str, Any]:
        """
        Obtém estatísticas do arquivo.

        Args:
            file_path: Caminho para o arquivo

        Returns:
            Dicionário com estatísticas do arquivo
        """
        stat = file_path.stat()

        return {
            'path': str(file_path),
            'size_bytes': stat.st_size,
            'size_mb': round(stat.st_size / (1024 * 1024), 2),
            'created': datetime.fromtimestamp(stat.st_ctime).isoformat(),
            'modified': datetime.fromtimestamp(stat.st_mtime).isoformat()
        }

    def cleanup_old_files(
        self,
        pattern: str = "*.json",
        days_old: int = 7,
        directory: Optional[Path] = None
    ) -> int:
        """
        Remove arquivos mais antigos que o número especificado de dias.

        Args:
            pattern: Padrão glob
            days_old: Remover arquivos mais antigos que este número de dias
            directory: Diretório para limpar (padrão: raw_path)

        Returns:
            Número de arquivos removidos
        """
        directory = directory or self.raw_path
        now = datetime.utcnow().timestamp()
        max_age_seconds = days_old * 24 * 60 * 60

        removed_count = 0

        for file_path in directory.glob(pattern):
            file_age = now - file_path.stat().st_mtime

            if file_age > max_age_seconds:
                try:
                    file_path.unlink()
                    self.logger.info(f"Removed old file: {file_path}")
                    removed_count += 1
                except Exception as e:
                    self.logger.error(f"Failed to remove {file_path}: {e}")

        if removed_count > 0:
            self.logger.info(f"Cleaned up {removed_count} old files")

        return removed_count
