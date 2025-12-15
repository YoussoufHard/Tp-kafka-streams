#  TP Kafka Streams — Traitement de Flux de Données en Temps Réel

**Big Data Processing — 2025**
**Auteur : TANGARA Youssouf**
**Encadrant : Mr. Abdelmajid BOUSSELHAM**

---

# Présentation du TP

Ce projet regroupe **3 exercices complets** permettant de maîtriser Kafka et Kafka Streams à travers des cas concrets :

1. **Traitement et Nettoyage de Texte (Kafka Streams)**
2. **Analyse de Données Météorologiques**
3. **Application Spring Boot + Kafka Streams — Compteur de Clics**

Ce dépôt inclut également la configuration Kafka via Docker afin de faciliter l’exécution des exercices.

---

# 🧰 Prérequis

Avant de commencer, veuillez installer :

* **Docker + Docker Compose**
* **Java 17 ou 21**
* **Maven 3.9+**
* **IDE** : IntelliJ / VS Code / Eclipse
* **Git**

---

# 🏗️ Démarrage de Kafka via Docker

### 1. Lancer le cluster Kafka

Dans le répertoire `cluster-kafka/` :

```bash
docker-compose up -d
```

Vérifier que le broker est démarré :

```bash
docker ps
```

---

### 2. Accéder au conteneur Kafka

```bash
docker exec -it brokerkafka sh
```

Une fois dans le conteneur :

```bash
cd /opt/kafka/bin/
```

📌 *Toutes les commandes CLI sont à exécuter dans ce dossier.*

![conteneur creation](/captures/img.png)
---

# 📘 Exercice 1 — Traitement de Texte avec Kafka Streams

## 🎯 Objectif

Développer une application Kafka Streams capable de :

* Nettoyer des messages texte
* Supprimer les mots interdits
* Gestion via Dead Letter Queue
* Rediriger le texte filtré dans deux topics :

    * `text-clean`
    * `text-dead-letter`

---

# 1️⃣ Création des Topics

### Commandes à exécuter dans le conteneur Kafka :

```bash
./kafka-topics.sh --create --topic text-input --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
./kafka-topics.sh --create --topic text-clean --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
./kafka-topics.sh --create --topic text-dead-letter --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### Vérification des topics créés :

```bash
./kafka-topics.sh --list --bootstrap-server localhost:9092
```

📷 *Capture : Liste des topics*
![Liste des topics](/captures/img_1.png)

---

# 2️⃣ Test Manuel : Producer & Consumer

### Produire sur `text-input` :

```bash
./kafka-console-producer.sh --topic text-input --bootstrap-server localhost:9092
```

### Consommer :

```bash
./kafka-console-consumer.sh --topic text-input --bootstrap-server localhost:9092 --from-beginning
```

📷 *Capture : Test Producer/Consumer*
![Test Producer Consumer](/captures/img_2.png)

---

# 3️⃣ Lancement de l’Application Kafka Streams

Le projet contient une classe Java :

```
TextCleanerApp.java
```

Elle :

* crée automatiquement les topics (si absents)
* nettoie les messages
* applique les règles métier
* redirige vers les bons topics

### Lancer l'application :

```bash
mvn clean package
java -jar target/kafka-text-cleaner.jar
```

📷 *Capture : démarrage de l'application*
![Start App](/captures/img_3.png)

---

# 4️⃣ Résultats : Tests Fonctionnels

Après production de messages → observation des topics :
📷 *Capture : résultats text-clean & dead-letter*
![App Results](/captures/img_4.png)

---

#  Scénarios de Test comme sur la capture

| Message d’entrée            | Sortie attendue                    |
| --------------------------- | ---------------------------------- |
| `"  hello  world  "`        | `"HELLO WORLD"` → `text-clean`     |
| `""`                        | → `text-dead-letter`               |
| `"This is a HACK"`          | → `text-dead-letter`               |
| `"  multiple    spaces   "` | `"MULTIPLE SPACES"` → `text-clean` |
| message > 100 caractères    | → `text-dead-letter`               |

---

# 🧠 Fonctionnement Global

1. **Lecture** depuis `text-input`
2. **Nettoyage** : trim + espaces + majuscules
3. **Validation** selon règles :

    * pas vide
    * pas de mots interdits
    * long ≤ 100
4. **Routage** vers :

    * `text-clean`
    * `text-dead-letter`

---

---

# 📘 Exercice 2 — Analyse de Données Météorologiques

## 🎯 Objectif

Créer une application Kafka Streams pour analyser des relevés météo temps réel.

## 🔧 Spécifications

* Topic d'entrée : `weather-data`
* Format :

  ```
  station,temperature,humidity
  ```
* Étapes attendues :

    1. Lire les données → `KStream`
    2. Filtrer température > 30°C
    3. Convertir température en Fahrenheit
    4. Grouper par station
    5. Calculer moyennes (temp & humidité)
    6. Publier dans `station-averages`

## ➕ Implémentation

Le code est dans `WeatherAnalyzerApp.java`.

Il utilise Kafka Streams pour :

* Lire depuis `weather-data`
* Parser les données CSV
* Filtrer température > 30°C
* Convertir en Fahrenheit
* Grouper par station et calculer moyennes
* Publier vers `station-averages`

### Lancement

```bash
mvn clean package
java -cp target/classes net.youssouf.WeatherAnalyzerApp
```

### Création des topics

```bash
./kafka-topics.sh --create --topic weather-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
./kafka-topics.sh --create --topic station-averages --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

📷 *Capture : Topics météo*
![Topics Météo](/captures/img_5.png)

### Test

Produire des données :

```bash
./kafka-console-producer.sh --topic weather-data --bootstrap-server localhost:9092
```

Entrer :

```
Station1,25.3,60
Station2,35.0,50
Station2,40.0,45
```

Consommer les résultats :

```bash
./kafka-console-consumer.sh --topic station-averages --bootstrap-server localhost:9092 --from-beginning
```

📷 *Capture : Résultats météo exemple*
![Résultats Météo](/captures/img_6.png)

---

## 📊 Monitoring avec Prometheus & Grafana

### 🎯 Objectif

Intégrer Prometheus et Grafana pour monitorer en temps réel l'application Kafka Streams, observer les métriques JMX, et visualiser les performances (débit, latence, utilisation CPU/Mémoire).

### 🔧 Spécifications

* Activer JMX dans l'application Java
* Exposer les métriques via JMX Exporter
* Collecter avec Prometheus
* Visualiser avec Grafana

### ➕ Implémentation

L'application Kafka Streams intègre directement Prometheus pour exposer des métriques personnalisées (température et humidité moyennes par station) ainsi que les métriques JVM.

#### Métriques exposées

* `weather_avg_temperature_fahrenheit{station="..."}` : Température moyenne en Fahrenheit
* `weather_avg_humidity{station="..."}` : Humidité moyenne en pourcentage
* Métriques JVM (mémoire, CPU, etc.)

### Lancement

Démarrer l'application (les métriques sont exposées automatiquement sur le port 1234) :

```bash
java -cp target/classes net.youssouf.WeatherAnalyzerApp
```

Vérifier les métriques : http://localhost:1234/metrics

### Démarrage du Cluster Monitoring

Dans `cluster-prometheus/` :

```bash
docker login  # Si nécessaire pour éviter les erreurs d'authentification
docker-compose up -d
```

Vérifier les conteneurs :

```bash
docker ps
```

📷 *Capture : Cluster Prometheus & Grafana*
![Cluster Monitoring](/captures/img_7.png)

### Accès aux Interfaces

* Prometheus : http://localhost:9090
* Grafana : http://localhost:3000 (login: admin/admin)

📷 *Capture : Interface Prometheus*
![Prometheus](/captures/img_8.png)

📷 *Capture : Interface Gafana*
![Grafana](/captures/img_8_1.png)

### Configuration Grafana

1. Ajouter Prometheus comme source de données (URL: http://prometheus:9090)
2. Créer un dashboard avec les métriques suivantes :
   - Nombre de messages traités
   - Taux de messages filtrés
   - Latence Kafka Streams
   - Throughput (records/sec)
   - Memory / CPU usage

📷 *Capture : Dashboard Grafana*
![Dashboard Grafana](/captures/img_8_1.png)

### Test

Envoyer des données météo et observer les métriques en temps réel dans Grafana.

les metrics depuis http://localhost:1234/metrics

![Metrics](/captures/img_9.png)

La capture suivante est celui du test de certaines query prometheus

![Prometheus_query](/captures/img_10.png)
![Prometheus_query](/captures/img_11.png)

La capture de configuration de promethus comme datasource dans grafana
![Prometheus_query](/captures/img_12.png)

Capture dashboard grafana des différents metrics
![Grafana_visualisation](/captures/img_13.png)






---

# 📘 Exercice 3 — Application Spring Boot + Kafka Streams : Click Counter

*(Squelette également)*

## 🎯 Objectif

Développer une solution complète basée sur Kafka Streams et Spring Boot pour suivre et analyser les clics des utilisateurs en temps réel.

## 🔧 Architecture

* **Producteur Web** : Application Spring Boot avec interface web contenant un bouton "Cliquez ici" qui envoie des messages à Kafka.
* **Application Kafka Streams** : Consomme les messages du topic `clicks`, compte les clics par utilisateur, produit vers `click-counts`.
* **Consommateur REST** : Application Spring Boot qui consomme `click-counts` et expose une API REST GET `/clicks/count`.

## 📌 Implémentation

Trois applications séparées :

* `ClickProducerApp.java` : Producteur web Spring Boot
* `ClickStreamsApp.java` : Application Kafka Streams pour le comptage
* `ClickConsumerApp.java` : Consommateur REST Spring Boot

### Lancement

Lancer les trois applications séparément :

```bash
# Producteur (port 8080)
java -cp target/classes net.youssouf.ClickProducerApp

# Streams
java -cp target/classes net.youssouf.ClickStreamsApp

# Consommateur (port 8081)
java -cp target/classes -Dserver.port=8081 net.youssouf.ClickConsumerApp
```

### Création des topics

```bash
./kafka-topics.sh --create --topic clicks --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
./kafka-topics.sh --create --topic click-counts --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

📷 *Capture : Interface web*
![Interface Web](/captures/img_7.png)

### Test

Accéder à http://localhost:8080, cliquer sur le bouton.

Récupérer le compteur :

```bash
curl http://localhost:8081/clicks/count
```

📷 *Capture : Comptage en temps réel*
![Comptage](/captures/img_8.png)

---

# ▶️ Comment Lancer Tout le Projet

### 1. Démarrer Kafka

```bash
docker-compose up -d
```

### 2. Compiler l’application Kafka Streams

```bash
mvn clean package
```

### 3. Lancer l’exo 1 / exo 2 / exo 3

```bash
java -jar target/<nom-app>.jar
```

### 4. Tester avec Producer / Consumer

→ via commandes Kafka

---

# 📝 Notes Utile

### Supprimer un topic

```bash
./kafka-topics.sh --delete --topic <topic> --bootstrap-server localhost:9092
```

---

#  Conclusion

Ce TP permet de manipuler Kafka & Kafka Streams dans des scénarios variés :
✔ Nettoyage de texte
✔ Traitement météorologique
✔ Temps réel + API Spring Boot

