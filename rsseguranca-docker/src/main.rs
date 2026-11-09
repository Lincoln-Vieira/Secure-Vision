// ==========================================
// IMPORTAÇÕES E DEPENDÊNCIAS
// ==========================================

// Importação dos componentes principais do framework Axum para construção de serviços HTTP
use axum::{
    body::Body,
    extract::{Path, State},
    response::{AppendHeaders, IntoResponse},
    routing::{get, post},
    Router,
};

// Importação das traits fundamentais do GStreamer para controlo de estado e manipulação de elementos
use gstreamer::prelude::*;

// Importação da biblioteca Lapin para gestão de ligações, canais e publicação de mensagens no RabbitMQ
use lapin::{options::*, BasicProperties, Connection, ConnectionProperties};

// Importação de utilitários do SQLx para mapeamento de resultados do PostgreSQL em estruturas Rust
use sqlx::{FromRow, PgPool};

// Coleções e tipos de sincronização concorrente da biblioteca padrão do Rust
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

// Wrappers do Tokio para converter canais de broadcast em streams assíncronos
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

// Utilitário da camada Tower HTTP para servir ficheiros estáticos (imagens, CSS, JS)
use tower_http::services::ServeDir;

// ==========================================
// ESTRUTURAS DE DADOS DA APLICAÇÃO
// ==========================================

/// Estrutura para mapear os dados da câmara e do utilizador retornados diretamente do banco de dados PostgreSQL.
/// A macro `FromRow` do SQLx permite a conversão automática das colunas da consulta SQL para este struct.
#[derive(Debug, FromRow, Clone)]
struct CameraWithUser {
    /// Identificador único da câmara no banco de dados.
    nu_seg_camera: i32,
    /// Identificador único do utilizador proprietário da câmara.
    nu_seg_usuario: i32,
    /// Endereço IP ou hostname de rede da câmara IP/RTSP.
    no_seg_ip_camera: String,
    /// Nome de utilizador para autenticação no stream RTSP.
    no_seg_user_camera: String,
    /// Palavra-passe para autenticação no stream RTSP.
    no_seg_pass_word_camera: String,
    /// Nome completo do utilizador vindo da tabela de utilizadores.
    no_seg_nome: String,
}

/// Mantém os recursos em memória e o estado de execução de uma câmara ativa no servidor.
struct CameraSession {
    /// Nome do utilizador associado à câmara (utilizado para exibição no cabeçalho da interface).
    user_name: String,
    /// ID do utilizador associado à sessão.
    user_id: i32,
    /// ID da câmara associada à sessão.
    camera_id: i32,
    /// Canal de broadcast do Tokio utilizado para distribuir simultaneamente os frames JPEG capturados a múltiplos clientes HTTP.
    frame_tx: tokio::sync::broadcast::Sender<Arc<Vec<u8>>>,
    /// Objeto do pipeline do GStreamer em execução para este fluxo de vídeo.
    pipeline: gstreamer::Pipeline,
    /// Handle da task em background responsável por gerir a ligação e envio de frames para o RabbitMQ (permite abortar ao desligar).
    _rabbit_abort: tokio::task::JoinHandle<()>,
}

/// Estado global e partilhado da aplicação que é injetado nas rotas HTTP do Axum através da trait `Clone`.
#[derive(Clone)]
struct AppState {
    /// Mapa seguro para concorrência (com bloqueio de leitura/escrita `RwLock`) contendo as sessões de câmaras ativas.
    /// A chave do mapa é uma string no formato `"user_id:camera_id"`.
    cameras: Arc<RwLock<HashMap<String, CameraSession>>>,
    /// Pool de ligações com a base de dados PostgreSQL.
    db_pool: PgPool,
    /// Endereço da ligação AMQP com o broker RabbitMQ.
    amqp_addr: String,
}

// ==========================================
// FUNÇÃO PRINCIPAL (MAIN)
// ==========================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Carrega as variáveis de ambiente a partir do ficheiro .env se estiver presente no diretório raiz
    dotenvy::dotenv().ok();
    
    // Inicializa o motor do GStreamer antes de tentar construir qualquer pipeline de multimédia
    gstreamer::init()?;

    // Obtém o URL de ligação do PostgreSQL das variáveis de ambiente ou utiliza o valor por omissão para Docker
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://admseguranca:admseguranca@host.docker.internal:5432/dbseguranca".into());
    
    // Cria um pool de ligações assíncronas com o banco de dados PostgreSQL
    let db_pool = PgPool::connect(&database_url).await?;
    println!("Conectado ao PostgreSQL com sucesso!");

    // Obtém o endereço do servidor RabbitMQ a partir das variáveis de ambiente ou usa o padrão
    let amqp_addr = std::env::var("AMQP_ADDR")
        .unwrap_or_else(|_| "amqp://admseguranca:admseguranca@host.docker.internal:5672/%2f".into());

    // Inicializa o mapa de câmeras e constrói a estrutura do estado global da aplicação
    let cameras_map = HashMap::new();
    let app_state = AppState {
        cameras: Arc::new(RwLock::new(cameras_map)),
        db_pool: db_pool.clone(),
        amqp_addr: amqp_addr.clone(),
    };

    // Executa a sincronização inicial das câmeras ativas registadas na base de dados
    sync_cameras_internal(&app_state).await?;

    // Mapeia os endpoints HTTP do Axum e associa a pasta de estáticos no container
    let app = Router::new()
        .route("/", get(index_handler))
        .route("/video/:user_id/:camera_id", get(camera_view_handler))
        .route("/stream/:user_id/:camera_id", get(video_stream_handler))
        .route("/cameras/reload", post(reload_cameras_handler))
        .nest_service("/assets", ServeDir::new("/usr/src/app/assets")) // Caminho absoluto no container Docker
        .with_state(app_state);                                          // Injeta o estado global em todas as rotas

    // Inicializa o listener TCP na porta 3000 em todas as interfaces de rede disponíveis (0.0.0.0)
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Servidor web rodando em: http://127.0.0.1:3000");

    // Inicia o servidor HTTP assíncrono do Axum
    axum::serve(listener, app).await.unwrap();
    Ok(())
}

// ==========================================
// SINCRONIZAÇÃO E GESTÃO DAS CÂMERAS
// ==========================================

/// Consulta a base de dados para identificar as câmaras com serviço ativo (`nu_seg_status_servico = 1`).
/// Cria novos pipelines do GStreamer para câmaras recém-adicionadas e desliga câmaras desativadas.
async fn sync_cameras_internal(state: &AppState) -> Result<(), Box<dyn std::error::Error>> {
    // Executa a consulta na base de dados PostgreSQL trazendo as câmaras ativas e os nomes dos respetivos utilizadores
    let cameras_info: Vec<CameraWithUser> = sqlx::query_as(
        r#"
        SELECT 
            c.nu_seg_camera,
            c.nu_seg_usuario,
            c.no_seg_ip_camera, 
            c.no_seg_user_camera, 
            c.no_seg_pass_word_camera, 
            u.no_seg_nome 
        FROM seg_db.segtb002_cad_camera c
        JOIN seg_db.segtb001_usuario u ON c.nu_seg_usuario = u.nu_seg_usuario
        WHERE c.nu_seg_status_servico = 1
        "#
    )
    .fetch_all(&state.db_pool)
    .await
    .map_err(|e| format!("Erro ao buscar câmeras no banco: {}", e))?;

    // Adquire o bloqueio exclusivo de escrita no mapa global de câmeras
    let mut cameras = state.cameras.write().await;
    let mut new_keys = std::collections::HashSet::new();

    // Itera sobre o resultado do banco de dados para iniciar os fluxos necessários
    for cam in cameras_info {
        let user_id = cam.nu_seg_usuario;
        let camera_id = cam.nu_seg_camera;
        let key = format!("{}:{}", user_id, camera_id);
        new_keys.insert(key.clone());

        // Se a câmara já estiver ativa em execução, ignora a reinicialização para poupar recursos
        if cameras.contains_key(&key) {
            continue;
        }

        println!("Inicializando Câmera ID: {} para o Usuário: {} (ID: {})", camera_id, cam.no_seg_nome, user_id);

        let queue_name = format!("video_frames_u{}_c{}", user_id, camera_id);
        let rtsp_url = format!(
            "rtsp://{}:{}@{}:554/stream2",
            cam.no_seg_user_camera, cam.no_seg_pass_word_camera, cam.no_seg_ip_camera
        );

        // Canal broadcast do Tokio para retransmitir quadros JPEG para múltiplos navegadores via web
        let (frame_tx, _frame_rx) = tokio::sync::broadcast::channel::<Arc<Vec<u8>>>(16);
        let frame_tx_clone = frame_tx.clone();

        // String de montagem do pipeline GStreamer
        let pipeline_str = format!(
            "rtspsrc location={} latency=0 protocols=tcp ! rtph264depay ! h264parse ! decodebin ! videoconvert ! videoscale ! video/x-raw,width=640,height=360 ! jpegenc quality=60 ! appsink name=sink emit-signals=true sync=false async=false max-buffers=1 drop=true",
            rtsp_url
        );
        
        // Interpreta a string e constrói a estrutura do pipeline GStreamer
        let pipeline = match gstreamer::parse::launch(&pipeline_str) {
            Ok(p) => match p.downcast::<gstreamer::Pipeline>() {
                Ok(pipe) => pipe,
                Err(_) => continue,
            },
            Err(e) => {
                eprintln!("Erro ao criar pipeline GStreamer: {e}");
                continue;
            }
        };

        // Extrai o elemento 'appsink' definido na pipeline para registar callbacks de captura de quadros
        let appsink = match pipeline.by_name("sink").and_then(|s| s.downcast::<gstreamer_app::AppSink>().ok()) {
            Some(sink) => sink,
            None => {
                eprintln!("Elemento 'sink' não encontrado no pipeline!");
                continue;
            }
        };

        // Canal de mensagens MPSC para passar quadros entre a thread da câmera e a tarefa do RabbitMQ
        let (rabbit_tx, mut rabbit_rx) = tokio::sync::mpsc::channel::<Arc<Vec<u8>>>(30);
        let addr_bg = state.amqp_addr.clone();
        let queue_name_bg = queue_name.clone();

        // Subrotina em segundo plano que garante a reconexão automática e a publicação continuada no RabbitMQ
        let rabbit_abort = tokio::spawn(async move {
            loop {
                // Tenta estabelecer ligação com o RabbitMQ
                match Connection::connect(&addr_bg, ConnectionProperties::default()).await {
                    Ok(conn_bg) => {
                        if let Ok(channel_bg) = conn_bg.create_channel().await {
                            // Declara a fila de destino se esta ainda não existir
                            let _ = channel_bg
                                .queue_declare(
                                    &queue_name_bg,
                                    QueueDeclareOptions { auto_delete: true, ..Default::default() },
                                    lapin::types::FieldTable::default(),
                                )
                                .await;

                            // Consome quadros recebidos do canal e envia para a fila AMQP
                            while let Some(frame_arc) = rabbit_rx.recv().await {
                                if channel_bg
                                    .basic_publish(
                                        "",
                                        &queue_name_bg,
                                        BasicPublishOptions::default(),
                                        &frame_arc,
                                        BasicProperties::default(),
                                    )
                                    .await
                                    .is_err()
                                {
                                    // Se a publicação falhar, interrompe o loop interno para tentar reconectar a ligação AMQP
                                    break;
                                }
                            }
                        }
                    }
                    Err(_) => {
                        // Aguarda 5 segundos antes de tentar estabelecer uma nova ligação em caso de falha de rede
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    }
                }
            }
        });

        // Configura o callback disparado sempre que o GStreamer gera um novo quadro codificado em JPEG
        appsink.set_callbacks(
            gstreamer_app::AppSinkCallbacks::builder()
                .new_sample(move |sink| {
                    let sample = sink.pull_sample().map_err(|_| gstreamer::FlowError::Eos)?;
                    let buffer = sample.buffer().ok_or(gstreamer::FlowError::Error)?;
                    let map = buffer.map_readable().map_err(|_| gstreamer::FlowError::Error)?;
                    
                    let frame_arc = Arc::new(map.to_vec());

                    // Transmite o quadro para os utilizadores web ligados via broadcast HTTP
                    let _ = frame_tx_clone.send(frame_arc.clone());
                    // Envia uma cópia do ponteiro Arc para a task de publicação do RabbitMQ
                    let _ = rabbit_tx.try_send(frame_arc.clone());

                    Ok(gstreamer::FlowSuccess::Ok)
                })
                .build(),
        );

        // Altera o estado do pipeline multimédia para 'Playing', iniciando o consumo efetivo do RTSP
        if let Err(e) = pipeline.set_state(gstreamer::State::Playing) {
            eprintln!("Erro ao iniciar pipeline no estado Playing: {e}");
            rabbit_abort.abort();
            continue;
        }

        // Armazena a sessão ativa no mapa partilhado de câmaras
        cameras.insert(
            key,
            CameraSession {
                user_name: cam.no_seg_nome,
                user_id,
                camera_id,
                frame_tx,
                pipeline,
                _rabbit_abort: rabbit_abort,
            },
        );
    }

    // Identifica e limpa câmaras que foram desativadas na base de dados mas continuam em execução na memória
    let keys_to_remove: Vec<String> = cameras.keys().filter(|k| !new_keys.contains(*k)).cloned().collect();
    for key in keys_to_remove {
        if let Some(session) = cameras.remove(&key) {
            let _ = session.pipeline.set_state(gstreamer::State::Null);
            session._rabbit_abort.abort();
        }
    }

    Ok(())
}

// ==========================================
// HANDLERS HTTP (ROTAS AXUM)
// ==========================================

/// Endpoint `POST /cameras/reload`: Força a consulta à base de dados para atualizar os fluxos em execução.
async fn reload_cameras_handler(State(state): State<AppState>) -> impl IntoResponse {
    match sync_cameras_internal(&state).await {
        Ok(_) => (axum::http::StatusCode::OK, "Câmeras sincronizadas com sucesso!").into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("Erro ao sincronizar: {e}")).into_response(),
    }
}

/// Endpoint `GET /`: Redireciona o utilizador automaticamente para a visualização da primeira câmara disponível.
async fn index_handler(State(state): State<AppState>) -> impl IntoResponse {
    let cameras = state.cameras.read().await;
    if let Some((_, session)) = cameras.iter().next() {
        let target_url = format!("/video/{}/{}", session.user_id, session.camera_id);
        return axum::response::Html(format!(r#"<script>window.location.href="{}";</script>"#, target_url)).into_response();
    }
    axum::response::Html("<h1>Nenhuma câmera disponível.</h1>".to_string()).into_response()
}

/// Endpoint `GET /video/:user_id/:camera_id`: Renderiza a página com a imagem de fundo na tela cheia e o container da câmera no canto inferior esquerdo.
async fn camera_view_handler(
    State(state): State<AppState>,
    Path((path_user_id, path_camera_id)): Path<(i32, i32)>,
) -> impl IntoResponse {
    let key = format!("{}:{}", path_user_id, path_camera_id);
    let cameras = state.cameras.read().await;
    let session = match cameras.get(&key) {
        Some(s) => s,
        None => return axum::response::Html("<h1>Câmera não encontrada.</h1>".to_string()).into_response(),
    };

    let stream_url = format!("/stream/{}/{}", session.user_id, session.camera_id);
    let user_name = session.user_name.clone();
    let camera_id = session.camera_id;

    // Estrutura HTML/CSS: Imagem de fundo cobrindo toda a janela e container no canto inferior esquerdo
    let html_content = format!(r#"
        <!DOCTYPE html>
        <html lang="pt-BR">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Secure Vision - Câmera {}</title>
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
                    <h1>Câmera #{} — Usuário: {} Rust</h1>
                    <div class="badge-live">
                        <span class="dot"></span>
                        AO VIVO
                    </div>
                </div>
                <div class="video-wrapper" id="videoContainer">
                    <img src="{}" alt="Stream ao vivo..." id="streamImage" 
                         onerror="handleStreamError(this)" />
                </div>
                <div class="controls">
                    <span>Resolução: 640x360 | Formato: MJPEG</span>
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
    "#, camera_id, camera_id, user_name, stream_url);

    axum::response::Html(html_content).into_response()
}

/// Endpoint `GET /stream/:user_id/:camera_id`: Transmite o fluxo contínuo de vídeo em formato MJPEG (`multipart/x-mixed-replace`).
async fn video_stream_handler(
    State(state): State<AppState>,
    Path((path_user_id, path_camera_id)): Path<(i32, i32)>,
) -> impl IntoResponse {
    let key = format!("{}:{}", path_user_id, path_camera_id);
    
    // Subscreve o cliente ao canal broadcast da câmara solicitada
    let rx = {
        let cameras = state.cameras.read().await;
        match cameras.get(&key) {
            Some(s) => s.frame_tx.subscribe(),
            None => {
                return (
                    AppendHeaders([("Content-Type", "text/plain")]),
                    Body::from("Câmera não encontrada."),
                ).into_response();
            }
        }
    };

    // Converte os quadros recebidos no canal num stream HTTP delimitado por cabeçalhos multipart
    let stream = BroadcastStream::new(rx).filter_map(|result| {
        result.ok().map(|frame_arc| {
            let header = format!("--frame\r\nContent-Type: image/jpeg\r\nContent-Length: {}\r\n\r\n", frame_arc.len());
            let mut packet = header.into_bytes();
            packet.extend_from_slice(&frame_arc);
            packet.extend_from_slice(b"\r\n");
            Ok::<_, std::io::Error>(packet)
        })
    });

    // Retorna a resposta mantendo a ligação HTTP aberta continuamente
    (
        AppendHeaders([
            ("Content-Type", "multipart/x-mixed-replace; boundary=frame"),
            ("Cache-Control", "no-cache"),
            ("Connection", "keep-alive"),
        ]),
        Body::from_stream(stream),
    ).into_response()
}