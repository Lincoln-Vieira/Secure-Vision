# Usa a imagem oficial do Maven com JDK 21 (serve para compilar, desenvolver e rodar)
FROM maven:3.9.11-eclipse-temurin-21
WORKDIR /app

# Copia o pom.xml e baixa as dependências (otimiza o cache do Docker)
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Copia o restante do código fonte
COPY src ./src

# Expõe a porta da aplicação Spring Boot
EXPOSE 8085

# Comando padrão para iniciar a aplicação em modo de desenvolvimento
CMD ["mvn", "spring-boot:run"]