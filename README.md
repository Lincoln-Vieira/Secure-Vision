# Secure Vision

Sistema acadêmico de monitoramento inteligente que utiliza visão computacional para analisar gestos definidos pela aplicação e encaminhar eventos de alerta.

## Objetivo

Demonstrar uma arquitetura integrada de captura de vídeo, processamento de imagens, mensageria, persistência e notificações.

## Arquitetura resumida

1. **Rust**: serviço de captura/publicação dos quadros de vídeo.
2. **RabbitMQ**: transporte assíncrono dos quadros e eventos.
3. **Python (Flask, OpenCV e MediaPipe)**: processamento das imagens, reconhecimento das regras gestuais e disponibilização do stream.
4. **Java (Spring Boot)**: APIs, autenticação, consulta de câmeras e integração dos alertas.
5. **PostgreSQL**: persistência dos dados.
6. **Evolution API**: integração para envio de mensagens pelo WhatsApp.
7. **Docker Compose**: execução coordenada dos serviços.

## Regras gestuais implementadas no processamento analisado

- Duas mãos abertas levantadas: classificação `PERIGO`.
- Uma mão levantada fazendo o gesto de OK: classificação `OK`.

O sistema reconhece condições específicas programadas. Não deve ser interpretado como detector universal de emergências.

## Execução

A partir da pasta que contém `jvseguranca-docker/`:

```powershell
Set-Location .\jvseguranca-docker
docker compose up -d
docker compose ps
```

Para parar:

```powershell
docker compose down
```

A aplicação web foi acessada no ambiente de desenvolvimento em `http://localhost:8085/`. O servidor Python utiliza a porta `5000` para as rotas de visualização, conforme a configuração do Compose.

## Observação sobre vídeo

A página `/view/{usuarioId}/{cameraId}` e o endpoint `/video_feed/{usuarioId}/{cameraId}` dependem do serviço de captura estar ativo e publicando quadros no RabbitMQ. Abrir a página não inicia, por si só, a câmera física.

## Observação sobre o campo demonstrativo de WhatsApp

O campo demonstrativo adicionado à tela de cadastro salva o número no `localStorage` do navegador. Ele não envia o número ao backend nem configura o destino real das notificações.

## Segurança

- Não publique arquivos `.env`, tokens, senhas ou chaves de API.
- Configure as variáveis de ambiente conforme um `.env.example` sem valores secretos.
- Proteja as rotas de vídeo e valide autorização antes de disponibilizar o sistema externamente.

## Documentação

Consulte `DOCUMENTACAO/Documentacao_Completa_Secure_Vision.docx` para proposta, requisitos, arquitetura, banco de dados, fluxos, testes, limitações e roteiro de apresentação.

## Status de validação

A documentação diferencia funcionalidades analisadas no código de testes ainda dependentes de câmera física. Antes de considerar o sistema pronto para produção, valide o fluxo ponta a ponta em ambiente controlado.
