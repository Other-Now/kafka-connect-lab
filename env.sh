# source me from Git Bash: portable JDK 21 + Maven + Kafka on this box
export JAVA_HOME="D:/self_projects/.tools/jdk-21.0.12.1+1"
export PATH="/d/self_projects/.tools/jdk-21.0.12.1+1/bin:/d/self_projects/.tools/apache-maven-3.9.11/bin:$PATH"
export KAFKA_HOME="D:/self_projects/.tools/kafka_2.13-4.1.2"
# Local Postgres (the agentq PG16 cluster on 5433); database kc, user/password kc
export KC_DB="jdbc:postgresql://localhost:5433/kc"
export KC_TEST_DB="jdbc:postgresql://localhost:5433/kc_test"
