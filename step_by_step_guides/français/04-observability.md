# Observabilité des Jobs et Gouvernance des Données dans CDP

![alt text](../../img/observability-slide.png)

![alt text](../../img/catalog-slide.png)

## Contenu

5. [Surveiller les jobs avec Cloudera Observability et CDE](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/english/part_03_observability.md#lab-5-monitoring-jobs-with-cloudera-observability-and-cde)
6. [Gouvernance des jobs Spark avec CDP Data Catalog](https://github.com/pdefusco/CDE_121_HOL/blob/main/step_by_step_guides/english/part_03_observability.md#lab-6-spark-job-governance-with-cdp-data-catalog)

## Lab 5. Surveiller les jobs avec Cloudera Observability et CDE

CDE offre une fonctionnalité intégrée d'observabilité des jobs incluant une interface de suivi des exécutions, l'interface Airflow, et la capacité de télécharger les métadonnées et les logs des jobs via l'API et la CLI CDE. De plus, les utilisateurs CDE peuvent tirer parti de Cloudera Observability, un service Cloudera qui vous aide à comprendre de manière interactive votre environnement, vos data services, vos charges de travail, vos clusters et vos ressources sur l'ensemble des services de calcul d'un environnement CDP.

Lorsqu'une charge de travail se termine, des informations de diagnostic sur le job ou la requête et sur le cluster qui les a traités sont collectées par Telemetry Publisher et envoyées à Cloudera Observability, afin que vous puissiez optimiser vos requêtes et vos pipelines grâce à :

* Une large gamme de métriques et de tests de santé qui vous aident à identifier et à dépanner les problèmes existants et potentiels.
* Des recommandations et des conseils normatifs qui vous aident à résoudre rapidement ces problèmes et à optimiser les solutions.
* Des lignes de base de performance et une analyse historique qui vous aident à identifier et à résoudre les problèmes de performance.

De plus, Cloudera Observability vous permet également de :

* Afficher visuellement les coûts actuels et historiques de votre cluster de charges de travail pour vous aider à planifier et prévoir les budgets, les futurs environnements de charges de travail, et justifier les groupes d'utilisateurs et les ressources actuels.
* Déclencher des actions en temps réel à travers les jobs et les requêtes qui vous aident à prendre des mesures pour atténuer les problèmes potentiels.
* Activer la livraison quotidienne de vos statistiques de cluster à votre adresse e-mail, ce qui vous aide à suivre, comparer et surveiller sans avoir à vous connecter au cluster.
* Décomposer les métriques de vos charges de travail en vues plus significatives pour les besoins de votre entreprise, ce qui vous aide à analyser des critères spécifiques de charges de travail. Par exemple, vous pouvez analyser les performances des requêtes qui accèdent à une base de données particulière ou qui utilisent un pool de ressources spécifique par rapport à vos SLA. Vous pouvez aussi examiner les performances de toutes les requêtes exécutées sur votre cluster envoyées par un utilisateur spécifique.

#### Identifier la cause d'un ralentissement inhabituel des jobs Spark CDE dans CDP Observability

Sortez de CDE et retournez sur la page d'accueil CDP, puis ouvrez CDP Observability. Sélectionnez et développez le cluster virtuel Org1, puis l'onglet "Spark". Recherchez l'application Spark "LargeShuffleExample" et identifiez les jobs qui prennent plus de temps que d'habitude. À quelle fréquence le job s'exécute-t-il plus lentement que d'habitude ?

![alt text](../../img/obs-main-page.png)

![alt text](../../img/obs-slow-jobs.png)

![alt text](../../img/obs-examine-job.png)

Sélectionnez l'exécution du job ayant la durée la plus élevée et explorez l'onglet Execution Details pour trouver des informations au niveau des jobs et des étapes Spark, ainsi que l'onglet Baseline pour trouver des métriques d'exécution Spark granulaires. Dans l'onglet Baseline, cliquez sur l'icône "Show Abnormal Metrics" pour identifier les problèmes potentiels avec votre exécution de job particulière.

![alt text](../../img/details-1.png)

![alt text](../../img/details-2.png)

![alt text](../../img/details-3.png)

En inspectant les métriques de l'exécution en cours et en les comparant à la ligne de base, il semble qu'environ 20 % du temps, l'application effectue un Spark Shuffle anormal. Ensuite, ouvrez le code de l'application Spark et essayez d'identifier pourquoi cela se produit. Le code se trouve dans ["observability/skewApp.py"](https://github.com/pdefusco/CDE_123_HOL/blob/main/observability/skewApp.py)

#### Identifier la cause d'un échec d'un job Spark CDE dans CDP Observability

Passez maintenant au cluster virtuel Org2 dans Observability et ouvrez la vue des échecs de jobs. Identifiez une exécution échouée du job "ObsDemo" et explorez la trace d'erreur.

![alt text](../../img/obs-failed-1.png)

![alt text](../../img/obs-failed-2.png)

![alt text](../../img/obs-failed-3.png)

![alt text](../../img/obs-failed-4.png)

![alt text](../../img/obs-failed-5.png)

Il semble que votre job Spark ait échoué en raison de ressources insuffisantes. En particulier, l'une de vos partitions contient trop de données à cause d'un skew. Afin de relancer le job avec succès, vous pourriez simplement augmenter la mémoire et les cœurs de l'exécuteur Spark, ou vous pourriez améliorer le code pour mieux gérer le skew des données.

## Lab 6. Gouvernance des jobs Spark avec CDP Data Catalog

CDP Data Catalog est un service au sein de CDP qui vous permet de comprendre, gérer, sécuriser et gouverner les actifs de données à l'échelle de l'entreprise. Data Catalog vous aide à comprendre les données à travers plusieurs clusters et plusieurs environnements CDP. En utilisant Data Catalog, vous pouvez comprendre comment les données sont interprétées pour être utilisées, comment elles sont créées et modifiées, et comment l'accès aux données est sécurisé et protégé.

#### Explorer les jobs dans Apache Atlas

Retournez à la page d'accueil CDP, ouvrez Data Catalog, puis Atlas.

![alt text](../../img/catalog_1.png)

![alt text](../../img/catalog_2.png)

Atlas représente les métadonnées sous forme de types et d'entités, et fournit des capacités de gestion et de gouvernance des métadonnées pour permettre aux organisations de construire, catégoriser et gouverner leurs actifs de données.

Recherchez "spark_applications" dans la barre de recherche, puis sélectionnez une application Spark dans la liste et explorez ses métadonnées.

![alt text](../../img/catalog_3.png)

![alt text](../../img/catalog_4.png)

Dans le volet Classifications, créez une nouvelle classification de métadonnées. Assurez-vous d'utiliser un nom unique.

![alt text](../../img/catalog_5.png)

Retournez à la page principale, trouvez une application Spark et ouvrez-la. Appliquez ensuite la classification de métadonnées nouvellement créée.

![alt text](../../img/catalog_6.png)

![alt text](../../img/catalog_7.png)

Enfin, effectuez une nouvelle recherche, cette fois en utilisant la classification que vous avez créée afin de filtrer les applications Spark.

![alt text](../../img/catalog_8.png)

## Résumé

Cloudera Observability est la solution d'observabilité de type "single pane of glass" de CDP, qui découvre et collecte en continu la télémétrie de performance à travers les composants de données, d'applications et d'infrastructure exécutés dans les déploiements CDP sur les clouds privés et publics. Grâce à des analyses avancées et intelligentes et à des corrélations, elle fournit des informations et des recommandations pour résoudre les problèmes délicats, optimiser les coûts et améliorer les performances.

CDP Data Catalog est un catalogue de données cloud, c'est-à-dire un service de gestion des métadonnées qui aide les organisations à trouver, gérer et comprendre leurs données dans le cloud. C'est un référentiel centralisé qui peut aider à la prise de décision basée sur les données, améliorer la gestion des données et augmenter l'efficacité opérationnelle.

Dans cette dernière section des labs, vous avez exploré les capacités de surveillance des exécutions de jobs dans CDE. En particulier, vous avez utilisé l'interface des exécutions de jobs CDE pour conserver les métadonnées des exécutions, les logs Spark et l'interface Spark après exécution. Ensuite, vous avez utilisé CDP Observability pour explorer des métriques granulaires d'exécutions de jobs et détecter les valeurs aberrantes. Enfin, vous avez utilisé CDP Data Catalog afin de classifier les exécutions de jobs Spark pour gouverner et rechercher des métadonnées importantes.

## Liens utiles et ressources

* [Documentation Cloudera Observability](https://docs.cloudera.com/observability/cloud/index.html)
* [CDP Data Catalog](https://docs.cloudera.com/data-catalog/cloud/index.html)
* [Documentation Apache Atlas](https://docs.cloudera.com/cdp-reference-architectures/latest/cdp-ra-security/topics/cdp-ra-security-apache-atlas.html)
* [Documentation Apache Ranger](https://docs.cloudera.com/cdp-reference-architectures/latest/cdp-ra-security/topics/cdp-ra-security-apache-ranger.html)
* [Surveiller efficacement les jobs, exécutions et ressources avec le CDE CLI](https://community.cloudera.com/t5/Community-Articles/Efficiently-Monitoring-Jobs-Runs-and-Resources-with-the-CDE/ta-p/379893)
