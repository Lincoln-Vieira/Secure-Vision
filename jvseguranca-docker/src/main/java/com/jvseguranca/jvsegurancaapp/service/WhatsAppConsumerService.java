
package com.jvseguranca.jvsegurancaapp.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class WhatsAppConsumerService {

    private final JdbcTemplate jdbcTemplate;
    private final ConnectionFactory connectionFactory;
    private final RestClient restClient;
    private final String evolutionApiInstance;

    private final Map<String, SimpleMessageListenerContainer> containersAtivos =
            new ConcurrentHashMap<>();

    public WhatsAppConsumerService(
            JdbcTemplate jdbcTemplate,
            ConnectionFactory connectionFactory,
            @Value("${evolution.api.url:http://whatsapp-api:8080}") String evolutionApiUrl,
            @Value("${evolution.api.token}") String evolutionApiKey,
            @Value("${evolution.api.instance:wpseguranca}") String evolutionApiInstance) {

        this.jdbcTemplate = jdbcTemplate;
        this.connectionFactory = connectionFactory;
        this.evolutionApiInstance = evolutionApiInstance;

        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .build();

        JdkClientHttpRequestFactory requestFactory =
                new JdkClientHttpRequestFactory(httpClient);

        requestFactory.setReadTimeout(Duration.ofSeconds(60));

        this.restClient = RestClient.builder()
                .baseUrl(evolutionApiUrl)
                .requestFactory(requestFactory)
                .defaultHeader("apikey", evolutionApiKey)
                .defaultHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                .build();
    }

    @PostConstruct
    public void registrarListenersDoBanco() {
        recarregarFilasAtivas();
    }

    public synchronized int recarregarFilasAtivas() {
        System.out.println(">>> [RABBITMQ] Recarregando filas ativas...");

        pararTodosContainers();

        try {
            String sql = "SELECT nu_seg_usuario, nu_seg_camera, nu_seg_pais, "
                    + "nu_seg_ddd, nu_seg_celular, no_seg_grupo, "
                    + "ic_seg_tipo_whatsapp "
                    + "FROM seg_db.segtb003_cad_whatsapp "
                    + "WHERE nu_seg_status_servico = 1";

            List<Map<String, Object>> filasConfigs =
                    jdbcTemplate.queryForList(sql);

            int totalRegistrados = 0;

            for (Map<String, Object> config : filasConfigs) {
                String nomeFila = "alertas_u"
                        + config.get("nu_seg_usuario")
                        + "_c"
                        + config.get("nu_seg_camera");

                String destino = resolverDestino(config);

                if (destino != null && !destino.isBlank()) {
                    registrarListenerDinamico(nomeFila, destino);
                    totalRegistrados++;
                }
            }

            System.out.println(
                    ">>> [RABBITMQ] Listeners recarregados. Total: "
                            + totalRegistrados
            );

            return totalRegistrados;

        } catch (Exception e) {
            System.err.println(
                    ">>> [ERRO] Falha ao recarregar filas: " + e.getMessage()
            );

            throw new RuntimeException(
                    "Falha ao recarregar filas ativas", e
            );
        }
    }

    private String resolverDestino(Map<String, Object> config) {
        String tipoWhatsapp =
                (String) config.get("ic_seg_tipo_whatsapp");

        if ("CELULAR".equalsIgnoreCase(tipoWhatsapp)) {
            Object pais = config.get("nu_seg_pais");
            Object ddd = config.get("nu_seg_ddd");
            Object celular = config.get("nu_seg_celular");

            if (pais != null && ddd != null && celular != null) {
                return pais.toString()
                        + ddd.toString()
                        + celular.toString();
            }

        } else if ("GRUPO".equalsIgnoreCase(tipoWhatsapp)) {
            Object grupo = config.get("no_seg_grupo");

            if (grupo != null) {
                return grupo.toString();
            }
        }

        return null;
    }

    public void registrarListenerDinamico(
            String nomeFila,
            String numeroDestino) {

        if (containersAtivos.containsKey(nomeFila)) {
            pararContainer(nomeFila);
        }

        SimpleMessageListenerContainer container =
                new SimpleMessageListenerContainer(connectionFactory);

        container.setQueueNames(nomeFila);
        container.setConcurrentConsumers(1);
        container.setMaxConcurrentConsumers(3);
        container.setMissingQueuesFatal(false);

        container.setMessageListener(message -> {
            try {
                String corpoMensagem = new String(
                        message.getBody(),
                        StandardCharsets.UTF_8
                );

                System.out.println(
                        ">>> [RABBITMQ] Mensagem recebida na fila: "
                                + nomeFila
                );

                salvarHistoricoAlerta(nomeFila, corpoMensagem);
                processarEEnviarAlerta(numeroDestino, corpoMensagem);

            } catch (Exception e) {
                System.err.println(
                        ">>> [ERRO] Falha ao processar mensagem da fila "
                                + nomeFila + ": " + e.getMessage()
                );
            }
        });

        container.start();
        containersAtivos.put(nomeFila, container);
    }

    private void pararContainer(String nomeFila) {
        SimpleMessageListenerContainer container =
                containersAtivos.remove(nomeFila);

        if (container != null) {
            try {
                container.stop();
                container.destroy();

            } catch (Exception e) {
                System.err.println(
                        ">>> [ERRO] Falha ao parar container: "
                                + e.getMessage()
                );
            }
        }
    }

    private void pararTodosContainers() {
        for (String nomeFila : containersAtivos.keySet()) {
            pararContainer(nomeFila);
        }

        containersAtivos.clear();
    }

    @PreDestroy
    public void onDestroy() {
        pararTodosContainers();
    }

    private void salvarHistoricoAlerta(
            String nomeFila,
            String mensagemTexto) {

        java.util.regex.Matcher matcher =
                java.util.regex.Pattern
                        .compile("alertas_u(\\d+)_c(\\d+)")
                        .matcher(nomeFila);

        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    "Nome de fila inválido: " + nomeFila
            );
        }

        int usuarioId = Integer.parseInt(matcher.group(1));
        int cameraId = Integer.parseInt(matcher.group(2));

        String sql = "INSERT INTO seg_db.segtb006_historico_alerta "
                + "(nu_seg_usuario, nu_seg_camera, no_seg_mensagem_alerta) "
                + "VALUES (?, ?, ?)";

        jdbcTemplate.update(
                sql,
                usuarioId,
                cameraId,
                mensagemTexto
        );

        System.out.println(
                ">>> [HISTÓRICO] Alerta salvo no banco. Usuário: "
                        + usuarioId + " | Câmera: " + cameraId
        );
    }

    public void processarEEnviarAlerta(
            String numeroDestino,
            String mensagemTexto) {

        try {
            Map<String, String> payloadMap = new HashMap<>();

            payloadMap.put("number", numeroDestino);
            payloadMap.put("text", mensagemTexto);

            ObjectMapper mapper = new ObjectMapper();
            String jsonBody = mapper.writeValueAsString(payloadMap);

            var response = restClient.post()
                    .uri("/message/sendText/" + evolutionApiInstance)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(jsonBody)
                    .retrieve()
                    .toBodilessEntity();

            System.out.println(
                    ">>> [SUCESSO] Envio realizado. Status HTTP: "
                            + response.getStatusCode()
            );

        } catch (RestClientResponseException e) {
            System.err.println(
                    ">>> [ERRO HTTP] Evolution API: "
                            + e.getStatusCode().value()
            );

        } catch (Exception e) {
            System.err.println(
                    ">>> [ERRO] Falha no envio: " + e.getMessage()
            );

            throw new RuntimeException(
                    "Falha de envio para a Evolution API", e
            );
        }
    }
}
