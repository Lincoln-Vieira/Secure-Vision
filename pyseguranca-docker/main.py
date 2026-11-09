# ==========================================
# IMPORTAÇÕES E DEPENDÊNCIAS
# ==========================================

import os  # Para leitura de variáveis de ambiente do sistema
import threading  # Para gerenciar threads paralelas de consumo e processamento por câmera
import time  # Para controle de intervalos, cooldowns e pausas de reconexão
import cv2  # OpenCV para manipulação, decodificação e desenho em imagens/frames
from flask import Flask, Response, jsonify, render_template_string, send_from_directory
import mediapipe as mp  # Biblioteca do Google para rastreamento de poses e mãos (IA)
import numpy as np  # Manipulação eficiente de arrays numéricos e cálculos matemáticos
import pika  # Cliente Python para comunicação síncrona/assíncrona com o RabbitMQ
import pika.exceptions
import psycopg2  # Driver de conexão com o banco de dados PostgreSQL

# ==========================================
# CONFIGURAÇÕES GLOBAIS E DO APLICATIVO
# ==========================================

# Inicialização do aplicativo Flask
app = Flask(__name__)

# Dicionários globais protegidos e compartilhados entre as threads de IA e as rotas HTTP
output_frames = (
    {}
)  # Mapeia "user_id_camera_id" -> último frame processado (com desenhos e alertas)
frame_locks = (
    {}
)  # Mapeia "user_id_camera_id" -> Thread Lock específico para evitar concorrência de leitura/escrita no frame
last_alert_times = (
    {}
)  # Mapeia "user_id_camera_id" -> timestamp do último alerta enviado (para respeitar o cooldown)
active_threads = (
    {}
)  # Mapeia "user_id_camera_id" -> Objeto Thread ativo em background
thread_lock = (
    threading.Lock()
)  # Lock global para proteger o dicionário active_threads contra acessos concorrentes

# Inicialização dos módulos de soluções do MediaPipe
mp_drawing = (
    mp.solutions.drawing_utils
)  # Utilitário para desenhar os esqueletos e landmarks na imagem
mp_pose = mp.solutions.pose  # Modelo de detecção de esqueleto e pose corporal
mp_hands = (
    mp.solutions.hands
)  # Modelo de detecção de palmas e pontos dos dedos das mãos

# URLs de conexão obtidas via variáveis de ambiente (com fallbacks para Docker Compose local)
RABBITMQ_URL = os.getenv(
    "AMQP_ADDR",
    "amqp://admseguranca:admseguranca@host.docker.internal:5672/%2f",
)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgres://admseguranca:admseguranca@host.docker.internal:5432/dbseguranca",
)
ALERT_COOLDOWN = 2.0  # Intervalo mínimo em segundos entre envios consecutivos de alertas para a mesma câmera

# ==========================================
# FUNÇÕES DE BANCO DE DADOS E MENSAGERIA
# ==========================================


def get_active_cameras():
  """Conecta ao PostgreSQL e busca todas as câmeras cadastradas

  que possuem o status de serviço ativo (nu_seg_status_servico = 1).
  """
  try:
    connection = psycopg2.connect(DATABASE_URL)
    cursor = connection.cursor()
    cursor.execute("""
            SELECT c.nu_seg_camera, c.nu_seg_usuario, u.no_seg_nome 
            FROM seg_db.segtb002_cad_camera c
            JOIN seg_db.segtb001_usuario u ON c.nu_seg_usuario = u.nu_seg_usuario
            WHERE c.nu_seg_status_servico = 1
        """)
    cameras = cursor.fetchall()
    cursor.close()
    connection.close()
    return cameras
  except Exception as e:
    print(f"[Erro ao buscar câmeras no banco]: {e}")
    return []


def send_rabbitmq_alert(alert_queue_name, message):
  """Conecta ao RabbitMQ de forma pontual e publica uma mensagem de alerta

  na fila dedicada correspondente à câmera/usuário (com entrega durável).
  """
  try:
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    # Declara a fila de alertas como durável para garantir resiliência caso o broker reinicie
    channel.queue_declare(queue=alert_queue_name, durable=True)
    channel.basic_publish(
        exchange="",
        routing_key=alert_queue_name,
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2
        ),  # Torna a mensagem persistente
    )
    connection.close()
    print(f"[ALERTA ENVIADO para {alert_queue_name}]: {message}")
  except Exception as e:
    print(f"[Erro ao enviar alerta ao RabbitMQ]: {e}")


# ==========================================
# LÓGICA DE VISÃO COMPUTACIONAL E GESTOS
# ==========================================


def is_hand_open(hand_landmarks):
  """Analisa os landmarks de uma mão para verificar se ela está aberta

  (verificando se a ponta de pelo menos 4 dedos está acima da altura do pulso).
  """
  wrist = hand_landmarks.landmark[0]
  tips = [
      4,
      8,
      12,
      16,
      20,
  ]  # IDs das pontas: Polegar, Indicador, Médio, Anelar e Mínimo

  open_fingers = 0
  for tip_id in tips:
    tip = hand_landmarks.landmark[tip_id]
    # No eixo Y do OpenCV/MediaPipe, valores menores significam partes mais altas da imagem
    if tip.y < wrist.y:
      open_fingers += 1

  return open_fingers >= 4


def is_diving_ok_sign(hand_landmarks):
  """Identifica o gesto de 'OK' (polegar e indicador encostados forming um círculo,

  com os outros três dedos estendidos para cima).
  """
  thumb_tip = hand_landmarks.landmark[4]
  index_tip = hand_landmarks.landmark[8]
  wrist = hand_landmarks.landmark[0]

  # Calcula a distância euclidiana entre a ponta do polegar e a ponta do indicador
  thumb_index_dist = np.sqrt(
      (thumb_tip.x - index_tip.x) ** 2 + (thumb_tip.y - index_tip.y) ** 2
  )
  is_circle = (
      thumb_index_dist < 0.08
  )  # Limiar de proximidade para considerar o círculo do OK

  finger_tips = [12, 16, 20]
  finger_pips = [10, 14, 18]
  extended_fingers = 0
  for tip_id, pip_id in zip(finger_tips, finger_pips):
    tip = hand_landmarks.landmark[tip_id]
    pip = hand_landmarks.landmark[pip_id]
    if tip.y < pip.y and tip.y < wrist.y:
      extended_fingers += 1

  return is_circle and (extended_fingers == 3)


def are_hands_raised(hands_results, pose_results):
  """Verifica se todas as mãos detectadas estão levantadas acima da linha dos ombros do esqueleto detectado."""
  if not pose_results.pose_landmarks or not hands_results.multi_hand_landmarks:
    return False

  landmarks = pose_results.pose_landmarks.landmark
  left_shoulder_y = landmarks[mp_pose.PoseLandmark.LEFT_SHOULDER].y
  right_shoulder_y = landmarks[mp_pose.PoseLandmark.RIGHT_SHOULDER].y
  shoulder_level = min(
      left_shoulder_y, right_shoulder_y
  )  # Pega o ombro mais alto como referência

  for hand in hands_results.multi_hand_landmarks:
    wrist_y = hand.landmark[0].y
    if (
        wrist_y > shoulder_level
    ):  # Se o pulso estiver abaixo do ombro, a mão não está levantada
      return False

  return True


def check_gestures(hands_results, pose_results):
  """Combina pose e gestos das mãos para determinar se há uma situação de PERIGO

  (duas mãos abertas para o alto) ou STATUS OK.
  """
  if not hands_results.multi_hand_landmarks or not pose_results.pose_landmarks:
    return None, None

  if not are_hands_raised(hands_results, pose_results):
    return None, None

  hands = hands_results.multi_hand_landmarks
  num_hands = len(hands)

  # Condição de Perigo: 2 mãos levantadas e ambas abertas
  if num_hands == 2:
    if is_hand_open(hands[0]) and is_hand_open(hands[1]):
      return "PERIGO", "ALERTA: Perigo detectado (Duas mãos abertas para cima)!"

  # Condição de OK: 1 mão levantada fazendo o gesto de OK
  if num_hands == 1 and is_diving_ok_sign(hands[0]):
    return "OK", "STATUS: Tudo bem (Sinal de OK para cima)!"

  return None, None


# ==========================================
# WORKER DE CONSUMO E PROCESSAMENTO EM BACKGROUND
# ==========================================


def consume_camera_stream(user_id, camera_id, user_name):
  """Função executada em thread dedicada por câmera. Conecta-se à fila de vídeo do RabbitMQ,

  decodifica os frames JPEGs enviados pelo Rust, processa IA com MediaPipe, desenha
  os esqueletos/alertas e gerencia o envio de eventos para a fila de alertas.
  """
  global output_frames, frame_locks, last_alert_times

  key = f"{user_id}_{camera_id}"
  video_queue_name = f"video_frames_u{user_id}_c{camera_id}"
  alert_queue_name = f"alertas_u{user_id}_c{camera_id}"

  # Inicializa primitivas de concorrência locais para esta câmera
  frame_locks[key] = threading.Lock()
  last_alert_times[key] = 0

  connection_parameters = pika.URLParameters(RABBITMQ_URL)

  # Loop principal com resiliência para reconexão automática caso o RabbitMQ caia
  while True:
    try:
      print(
          f"[*] Conectando ao RabbitMQ para consumir a fila {video_queue_name}..."
      )
      connection = pika.BlockingConnection(connection_parameters)
      channel = connection.channel()
      channel.queue_declare(queue=alert_queue_name, durable=True)
      channel.basic_qos(
          prefetch_count=1
      )  # Garante que processa um frame por vez de forma justa

      # Inicializa os modelos do MediaPipe com complexidade leve (model_complexity=0) para otimizar CPU
      with (
          mp_pose.Pose(
              model_complexity=0,
              min_detection_confidence=0.5,
              min_tracking_confidence=0.5,
          ) as pose,
          mp_hands.Hands(
              model_complexity=0,
              max_num_hands=2,
              min_detection_confidence=0.5,
              min_tracking_confidence=0.5,
          ) as hands,
      ):

        def callback(ch, method, properties, body):
          global output_frames, last_alert_times
          try:
            # Converte os bytes recebidos do RabbitMQ em um array NumPy e decodifica como imagem OpenCV
            np_arr = np.frombuffer(body, dtype=np.uint8)
            frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

            if frame is None:
              ch.basic_ack(delivery_tag=method.delivery_tag)
              return

            # Redimensiona o frame para otimizar o processamento da IA
            ## frame = cv2.resize(frame, (480, 270))
            image_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            image_rgb.flags.writeable = (
                False  # Otimização de performance para o MediaPipe
            )

            # Executa o pipeline de IA (Pose e Mãos)
            pose_results = pose.process(image_rgb)
            hands_results = hands.process(image_rgb)

            image_rgb.flags.writeable = True
            image = cv2.cvtColor(image_rgb, cv2.COLOR_RGB2BGR)

            # Desenha os landmarks do esqueleto se encontrados
            if pose_results.pose_landmarks:
              mp_drawing.draw_landmarks(
                  image, pose_results.pose_landmarks, mp_pose.POSE_CONNECTIONS
              )

            # Desenha os landmarks das mãos se encontrados
            if hands_results.multi_hand_landmarks:
              for hand_landmarks in hands_results.multi_hand_landmarks:
                mp_drawing.draw_landmarks(
                    image, hand_landmarks, mp_hands.HAND_CONNECTIONS
                )

            # Verifica se algum gesto de perigo ou ok foi executado
            status_type, message = check_gestures(hands_results, pose_results)

            # Se houver um alerta válido e o cooldown tiver expirado, publica no RabbitMQ
            if message:
              current_time = time.time()
              if current_time - last_alert_times[key] > ALERT_COOLDOWN:
                send_rabbitmq_alert(
                    alert_queue_name,
                    f"[Usuário: {user_name} | Câmera: {camera_id}] {message}",
                )
                last_alert_times[key] = current_time

            # Escreve o texto de status visual no frame processado
            text_color = (
                (0, 0, 255) if status_type == "PERIGO" else (0, 255, 0)
            )
            cv2.putText(
                image,
                message if message else "",
                (20, 30),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.6,
                text_color,
                2,
            )

            # Salva o frame processado no dicionário global protegido por lock
            with frame_locks[key]:
              output_frames[key] = image.copy()

            # Confirma o recebimento e processamento da mensagem para o RabbitMQ (Manual ACK)
            ch.basic_ack(delivery_tag=method.delivery_tag)

          except Exception as e:
            print(f"[Erro no processamento da câmera {camera_id}]: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        # Configura o consumo assíncrono da fila de vídeo
        channel.basic_consume(
            queue=video_queue_name,
            on_message_callback=callback,
            auto_ack=False,
        )
        channel.start_consuming()

    except (pika.exceptions.AMQPError, Exception) as e:
      print(
          f"[Conexão perdida/Erro no RabbitMQ] Câmera {camera_id}: {e}. Tentando"
          " reconectar em 5 segundos..."
      )
      time.sleep(5)


def sync_and_start_cameras():
  """Sincroniza as câmeras ativas do banco de dados e inicializa novas threads

  de consumo e processamento de IA caso ainda não estejam rodando.
  """
  active_cameras = get_active_cameras()
  started_count = 0

  with thread_lock:
    for cam in active_cameras:
      cam_id, user_id, user_name = cam
      key = f"{user_id}_{cam_id}"

      # Se a thread não existe ou morreu, cria e dispara uma nova thread daemon
      if key not in active_threads or not active_threads[key].is_alive():
        t = threading.Thread(
            target=consume_camera_stream,
            args=(user_id, cam_id, user_name),
            daemon=True,
        )
        t.start()
        active_threads[key] = t
        started_count += 1
        print(
            f"-> [Reload] Thread iniciada para a Câmera ID {cam_id} do usuário"
            f" {user_name}"
        )

  return started_count


def generate_frames(user_id, camera_id):
  """Gerador de frames em formato MJPEG utilizado pelo Flask para fazer o streaming

  contínuo da imagem processada com IA direto para o navegador via HTTP
  multipart.
  """
  key = f"{user_id}_{camera_id}"
  while True:
    if key not in frame_locks:
      time.sleep(0.1)
      continue

    with frame_locks[key]:
      frame = output_frames.get(key)
      if frame is None:
        time.sleep(0.01)
        continue
      (flag, encodedImage) = cv2.imencode(".jpg", frame)
      if not flag:
        continue

    yield (
        b"--frame\r\n"
        b"Content-Type: image/jpeg\r\n\r\n" + bytearray(encodedImage) + b"\r\n"
    )
    time.sleep(0.01)


# ==========================================
# ROTAS DO SERVIDOR WEB (FLASK)
# ==========================================


@app.route("/assets/<path:filename>")
def serve_assets(filename):
  """Rota estática para servir a imagem de fundo /assets/imagem_frame.jpg."""
  return send_from_directory("/app/assets", filename)


@app.route("/")
def index():
  """Redireciona automaticamente para a visualização da primeira câmera disponível (espelhando o Rust)."""
  cameras = get_active_cameras()
  if cameras:
    cam_id, user_id, _ = cameras[0]
    target_url = f"/view/{user_id}/{cam_id}"
    return render_template_string(
        f'<script>window.location.href="{target_url}";</script>'
    )
  return render_template_string("<h1>Nenhuma câmera disponível.</h1>")


@app.route("/view/<int:user_id>/<int:camera_id>")
def view_camera(user_id, camera_id):
  """Renderiza a página com o mesmo visual do Rust: imagem de fundo cobrindo a tela e widget no canto inferior esquerdo."""
  # Obtém o nome do usuário associado à câmera
  cameras = get_active_cameras()
  user_name = "Desconhecido"
  for cam in cameras:
    if cam[0] == camera_id and cam[1] == user_id:
      user_name = cam[2]
      break

  stream_url = f"/video_feed/{user_id}/{camera_id}"

  # Template HTML idêntico ao do serviço Rust
  html_content = f"""
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Secure Vision - Câmera {camera_id}</title>
        <style>
            * {{
                box-sizing: border-box;
                margin: 0;
                padding: 0;
            }}
            /* Imagem imagem_frame.jpg cobrindo toda a tela (100vw x 100vh) */
            body {{
                width: 100vw;
                height: 100vh;
                background-image: url('/assets/imagem_frame.jpg');
                background-size: cover;
                background-position: center;
                background-repeat: no-repeat;
                background-color: #0d1117;
                color: #c9d1d9;
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
                display: flex;
                flex-direction: column;
                align-items: flex-start;  /* Posiciona no canto ESQUERDO */
                justify-content: flex-end; /* Posiciona na PARTE BAIXA */
                padding: 24px;             /* Distância em relação às bordas da tela */
                overflow: hidden;
            }}
            /* Container da câmera ajustado para o canto inferior esquerdo */
            .container {{
                width: 100%;
                max-width: 480px;          /* Tamanho ideal para widget no canto inferior */
                background: rgba(22, 27, 34, 0.95);
                border: 1px solid #30363d;
                border-radius: 12px;
                box-shadow: 0 12px 32px rgba(0, 0, 0, 0.85);
                overflow: hidden;
                backdrop-filter: blur(8px);
            }}
            .header {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 12px 16px;
                background-color: rgba(33, 38, 45, 0.9);
                border-bottom: 1px solid #30363d;
            }}
            .header h1 {{
                font-size: 0.95rem;
                font-weight: 600;
                color: #f0f6fc;
            }}
            .badge-live {{
                display: inline-flex;
                align-items: center;
                gap: 6px;
                background-color: rgba(238, 75, 43, 0.15);
                color: #ff4d4d;
                border: 1px solid rgba(238, 75, 43, 0.3);
                padding: 3px 8px;
                border-radius: 20px;
                font-size: 0.7rem;
                font-weight: 700;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }}
            .badge-live .dot {{
                width: 6px;
                height: 6px;
                background-color: #ff4d4d;
                border-radius: 50%;
                animation: pulse 1.5s infinite;
            }}
            @keyframes pulse {{
                0% {{ opacity: 1; transform: scale(1); }}
                50% {{ opacity: 0.4; transform: scale(0.9); }}
                100% {{ opacity: 1; transform: scale(1); }}
            }}
            /* Caixa de vídeo mantendo a proporção 16:9 */
            .video-wrapper {{
                position: relative;
                width: 100%;
                aspect-ratio: 16 / 9;
                background-color: #000;
                display: flex;
                justify-content: center;
                align-items: center;
            }}
            .video-wrapper img {{
                width: 100%;
                height: 100%;
                object-fit: contain;
            }}
            .controls {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 10px 16px;
                background-color: rgba(22, 27, 34, 0.9);
                border-top: 1px solid #30363d;
                font-size: 0.8rem;
                color: #8b949e;
            }}
            .btn-fullscreen {{
                background: #21262d;
                color: #c9d1d9;
                border: 1px solid #30363d;
                padding: 5px 12px;
                border-radius: 6px;
                cursor: pointer;
                font-size: 0.75rem;
                font-weight: 500;
                transition: background 0.2s, border-color 0.2s;
            }}
            .btn-fullscreen:hover {{
                background: #30363d;
                border-color: #8b949e;
                color: #fff;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Câmera #{camera_id} — Usuário: {user_name} Python</h1>
                <div class="badge-live">
                    <span class="dot"></span>
                    AO VIVO
                </div>
            </div>
            <div class="video-wrapper" id="videoContainer">
                <img src="{stream_url}" alt="Stream ao vivo..." id="streamImage" 
                     onerror="handleStreamError(this)" />
            </div>
            <div class="controls">
                <span>Resolução: 640x360 | Formato: MJPEG (IA)</span>
                <button class="btn-fullscreen" onclick="toggleFullscreen()">Tela Cheia</button>
            </div>
        </div>

        <script>
            function handleStreamError(img) {{
                setTimeout(() => {{
                    const currentSrc = img.src.split('?')[0];
                    img.src = currentSrc + '?t=' + new Date().getTime();
                }}, 3000);
            }}

            function toggleFullscreen() {{
                const container = document.getElementById('videoContainer');
                if (!document.fullscreenElement) {{
                    if (container.requestFullscreen) {{
                        container.requestFullscreen();
                    }} else if (container.webkitRequestFullscreen) {{
                        container.webkitRequestFullscreen();
                    }}
                }} else {{
                    if (document.exitFullscreen) {{
                        document.exitFullscreen();
                    }}
                }}
            }}
        </script>
    </body>
    </html>
    """
  return render_template_string(html_content)


@app.route("/video_feed/<int:user_id>/<int:camera_id>")
def video_feed(user_id, camera_id):
  """Endpoint HTTP responsável por transmitir o stream MJPEG contínuo ao navegador."""
  return Response(
      generate_frames(user_id, camera_id),
      mimetype="multipart/x-mixed-replace; boundary=frame",
  )


@app.route("/cameras/reload", methods=["POST"])
def reload_cameras_handler():
  """Endpoint POST para forçar a sincronização de novas câmeras ativas sem reiniciar o serviço."""
  try:
    new_started = sync_and_start_cameras()
    return (
        f"Câmeras sincronizadas com sucesso! Novas threads iniciadas:"
        f" {new_started}",
        200,
    )
  except Exception as e:
    return f"Erro ao sincronizar câmeras: {e}", 500


# ==========================================
# INICIALIZAÇÃO DA APLICAÇÃO
# ==========================================

if __name__ == "__main__":
  print("[*] Iniciando serviço de processamento de IA...")

  # Tenta sincronizar as câmeras uma vez, sem impedir o servidor HTTP de iniciar
  # quando ainda não há câmeras ativas ou o banco está temporariamente indisponível.
  try:
    initial_started = sync_and_start_cameras()
    print(f"[*] Câmeras iniciadas na inicialização: {initial_started}")
  except Exception as e:
    print(f"[Aviso] Não foi possível sincronizar as câmeras agora: {e}")

  # O servidor permanece disponível; POST /cameras/reload permite tentar novamente.
  app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
