from flask import Flask, jsonify, request
import pandas as pd
import math
from datetime import datetime

# Vamos reutilizar as funções e lógicas do seu script aqui
# Importação de funções (essas funções já estão no seu script original)
from alocacao_suplentes import get_google_sheet_data, upload_file_to_drive, normalizar_texto, classificar_cargo_padrao, contar_detalhado_efetivos_e_desvios, rodar_distribuicao, main

app = Flask(__name__)

@app.route('/executar', methods=['POST'])
def executar_script():
    try:
        # Chama o código principal para realizar a execução do script
        print("Iniciando a execução do script...")
        main()  # Aqui é onde a função main do seu script será executada.
        
        # Quando a execução do script for concluída, retorne uma resposta de sucesso
        return jsonify({'status': 'sucesso', 'message': 'Processamento concluído com sucesso.'}), 200
    except Exception as e:
        # Em caso de erro, retorne uma mensagem de erro
        return jsonify({'status': 'erro', 'message': str(e)}), 400

if __name__ == '__main__':
    app.run(debug=True)  # Roda o servidor Flask