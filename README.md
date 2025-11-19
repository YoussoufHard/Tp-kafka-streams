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

*(Squelette prêt à remplir lorsque tu feras l’exercice)*

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

## ➕ À implémenter

* Sérialisation personnalisée
* KGroupedStream + Aggregation
* Hook d’arrêt

## 📌 Exemple attendu

Input :

```
Station1,25.3,60
Station2,35.0,50
Station2,40.0,45
```

Output :

```
Station2 : Température Moyenne = 37.5°F | Humidité Moyenne = 47.5%
```

*(Tu ajouteras ton code + captures ici une fois terminé)*

---

# 📘 Exercice 3 — Application Spring Boot + Kafka Streams : Click Counter

*(Squelette également)*

## 🎯 Objectif

Créer une mini-application Web/REST avec :

* Un bouton qui envoie un “clic”
* Kafka Streams qui compte les clics en temps réel
* Une API REST qui expose les compteurs

## 🔧 Architecture

* **Frontend** → Envoie des clics
* **Kafka Producer** → topic `click-events`
* **Kafka Streams** → comptage `KTable`
* **Topic output** : `click-counts`
* **API REST Spring Boot** → expose `/stats`

## 📌 Fonctionnalités à implémenter

* Flux temps réel Kafka Streams
* State Store (RocksDB)
* REST Controller Spring
* Web interface simple (optionnel)

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

# 🎉 Conclusion

Ce TP permet de manipuler Kafka & Kafka Streams dans des scénarios variés :
✔ Nettoyage de texte
✔ Traitement météorologique
✔ Temps réel + API Spring Boot

Tu peux maintenant compléter ton dépôt GitHub avec un README clair, professionnel, parfaitement structuré pour un projet académique ou portfolio.

---

Si tu veux :
✅ Générer un PDF propre avec ce README
✅ Ajouter des badges GitHub
✅ Ajouter une architecture en diagrammes
Je peux aussi te les préparer.
