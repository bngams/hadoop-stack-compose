<center>![Batch Processing](img/batch.png)</center>


## Objectifs du TP
Initiation au framework hadoop et au patron MapReduce, utilisation de docker pour lancer un cluster hadoop de 3 noeuds.

## Outils et Versions
* [Apache Hadoop](http://hadoop.apache.org/) Version: 3.3.5
* [Docker](https://www.docker.com/) & Docker Compose Version *latest*
* [Visual Studio Code](https://code.visualstudio.com/) (ou tout autre IDE de votre choix)
* [Java](http://www.oracle.com/technetwork/java/javase/downloads/index.html) Version 11 ou supérieure
* Unix-like ou Unix-based Systems (Divers Linux et MacOS)

## Hadoop
### Présentation
[Apache Hadoop](hadoop.apache.org) est un framework open-source pour stocker et traiter les données volumineuses sur un cluster. Il est utilisé par un grand nombre de contributeurs et utilisateurs. Il a une licence Apache 2.0.

<center><img src="./img/tp1/hadoop.png" width="200"></center>


### Hadoop et Docker
Pour déployer le framework Hadoop, nous allons utiliser des contenaires [Docker](https://www.docker.com/) orchestrés avec Docker Compose. L'utilisation des contenaires va garantir la consistance entre les environnements de développement et permettra de réduire considérablement la complexité de configuration des machines (dans le cas d'un accès natif) ainsi que la lourdeur d'exécution (si on opte pour l'utilisation d'une machine virtuelle).

Notre architecture utilise l'image officielle Apache Hadoop (apache/hadoop:3.3.5).

### Installation
Nous allons utiliser tout au long de ces TP une architecture composée de:
- 1 NameNode (namenode): noeud maître pour HDFS
- 2 DataNodes (datanode1, datanode2): noeuds workers pour le stockage HDFS
- 1 ResourceManager (resourcemanager): gestionnaire de ressources YARN
- 1 NodeManager (nodemanager): gestionnaire d'exécution des tâches YARN

Vous devez pour cela avoir installé Docker et Docker Compose sur votre machine.

1. Se placer à la racine du projet où se trouve le fichier [docker-compose.yml](../../docker-compose.yml)

2. Lancer le cluster Hadoop avec Docker Compose:
```bash
docker compose up -d
```

3. Vérifier que les contenaires tournent bien:
```bash
docker compose ps
```

Vous devriez voir les services `namenode`, `datanode1`, `datanode2`, `resourcemanager` et `nodemanager` en état "running".

4. Entrer dans le contenaire namenode pour commencer à l'utiliser:
```bash
docker exec -it namenode bash
```

Le résultat de cette exécution sera le suivant:
```bash
root@namenode:~#
```

Vous vous retrouverez dans le shell du namenode. **Important**: Les services Hadoop (HDFS et YARN) démarrent automatiquement avec les contenaires, vous n'avez pas besoin de lancer de script manuellement.

??? info "Volumes et persistance des données"
    L'architecture utilise des volumes Docker pour persister les données:
    - `namenode_data`: stockage des métadonnées HDFS
    - `datanode1_data`, `datanode2_data`: stockage des blocs de données HDFS
    - Le répertoire `./data` de votre projet est monté dans les contenaires pour faciliter le transfert de fichiers

    Pour arrêter le cluster sans perdre les données:
    ```bash
    docker compose down
    ```

    Pour redémarrer le cluster:
    ```bash
    docker compose up -d
    ```

    Pour tout supprimer (contenaires + volumes):
    ```bash
    docker compose down -v
    ```

### Premiers pas avec Hadoop
Toutes les commandes interagissant avec le système HDFS commencent par ```hdfs dfs```. Ensuite, les options rajoutées sont très largement inspirées des commandes Unix standard.

  - Créer un répertoire dans HDFS, appelé _input_. Pour cela, taper:
```bash
hdfs dfs –mkdir -p input
```

??? bug "En cas d'erreur: _No such file or directory_"
    Si pour une raison ou une autre, vous n'arrivez pas à créer le répertoire _input_, avec un message ressemblant à ceci: ```ls: `.': No such file or directory```, veiller à construire l'arborescence de l'utilisateur principal (root), comme suit:

    ``` hdfs dfs -mkdir -p /user/root```

  - Nous allons utiliser le fichier  [purchases.txt](https://github.com/CodeMangler/udacity-hadoop-course/raw/master/Datasets/purchases.txt.gz) comme entrée pour le traitement MapReduce.
  - Commencer par décompresser le fichier sur votre machine, puis par le charger dans le contenaire ```namenode``` avec la commande suivante:
  ```Bash
  docker cp purchases.txt namenode:/root/purchases.txt
  ```
  - À partir du contenaire namenode, charger le fichier purchases dans le répertoire input (de HDFS) que vous avez créé:
  ```Bash
  hdfs dfs -put purchases.txt input
  ```
  - Pour afficher le contenu du répertoire _input_, la commande est:
  ```bash
  hdfs dfs –ls input
  ```
  - Pour afficher les dernières lignes du fichier purchases:
  ```bash
  hdfs dfs -tail input/purchases.txt
  ```

  Le résultat suivant va donc s'afficher:
    <center><img src="./img/tp1/purchases-tail.png"></center>



Nous présentons dans le tableau suivant les commandes les plus utilisées pour manipuler les fichiers dans HDFS:

|Instruction|Fonctionnalité|
|---------|-------------------------------------------------------------|
| ```hdfs dfs –ls``` | Afficher le contenu du répertoire racine |
| ```hdfs dfs –put file.txt``` | Upload un fichier dans hadoop (à partir du répertoire courant de votre disque local) |
| ```hdfs dfs –get file.txt``` | Download un fichier à partir de hadoop sur votre disque local |
| ```hdfs dfs –tail file.txt``` | Lire les dernières lignes du fichier   |
| ```hdfs dfs –cat file.txt``` | Affiche tout le contenu du fichier  |
| ```hdfs dfs –mv file.txt newfile.txt``` |  Renommer (ou déplacer) le fichier  |
| ```hdfs dfs –rm newfile.txt``` | Supprimer le fichier  |
| ```hdfs dfs –mkdir myinput``` | Créer un répertoire |


### Interfaces web pour Hadoop

Hadoop offre plusieurs interfaces web pour pouvoir observer le comportement de ses différentes composantes. Il est possible d'afficher ces pages directement sur notre machine hôte, et ce grâce à l'utilisation de l'option -p de la commande ```docker run```. En effet, cette option permet de publier un port du contenaire sur la machine hôte. Pour pouvoir publier tous les ports exposés, vous pouvez lancer votre contenaire en utilisant l'option ```-P```.

En regardant la commande ```docker run``` utilisée plus haut, vous verrez que deux ports de la machine maître ont été exposés:

  * Le port **9870**: qui permet d'afficher les informations de votre namenode.
  * Le port **8088**: qui permet d'afficher les informations du resource manager de Yarn et visualiser le comportement des différents jobs.

Une fois votre cluster lancé et hadoop démarré et prêt à l'emploi, vous pouvez, sur votre navigateur préféré de votre machine hôte, aller à : http://localhost:9870. Vous obtiendrez le résultat suivant:

![Namenode Info](img/tp1/namenode-info.png)

Vous pouvez également visualiser l'avancement et les résultats de vos Jobs (Map Reduce ou autre) en allant à l'adresse: http://localhost:8088.

![Resource Manager Info](img/tp1/resourceman-info.png)

## Map Reduce
### Présentation
Un Job Map-Reduce se compose principalement de deux types de programmes:

  - **Mappers** : permettent d’extraire les données nécessaires sous forme de clef/valeur, pour pouvoir ensuite les trier selon la clef
  - **Reducers** : prennent un ensemble de données triées selon leur clef, et effectuent le traitement nécessaire sur ces données (somme, moyenne, total...)

### Wordcount
Nous allons tester un programme MapReduce grâce à un exemple très simple, le _WordCount_, l'équivalent du _HelloWorld_ pour les applications de traitement de données. Le Wordcount permet de calculer le nombre de mots dans un fichier donné, en décomposant le calcul en deux étapes:

  * L'étape de _Mapping_, qui permet de découper le texte en mots et de délivrer en sortie un flux textuel, où chaque ligne contient le mot trouvé, suivi de la valeur 1 (pour dire que le mot a été trouvé une fois)
  * L'étape de _Reducing_, qui permet de faire la somme des 1 pour chaque mot, pour trouver le nombre total d'occurrences de ce mot dans le texte.

Commençons par créer un projet Maven dans VSCode. **Nous utiliserons dans notre cas JDK 11 ou supérieure**.

??? info "Version de JDK"
    Hadoop 3.3.5 supporte Java 8 et 11. Nous recommandons Java 11 pour une meilleure compatibilité avec les outils modernes. 

Pour créer un projet Maven dans VSCode: 

  * Prenez soin d'avoir les extensions _Maven for Java_ et _Extension Pack for Java_ activées.
  * Créer un nouveau répertoire dans lequel vous inclurez votre code.
  * Faites un clic-droit dans la fenêtre _Explorer_ et choisir *Create Maven Project*.  
  * Choisir _No Archetype_
  * Définir les valeurs suivantes pour votre projet:
    - **GroupId**: hadoop.mapreduce
    - **ArtifactId**: wordcount
    - **Version**: 1
  * Ouvrir le fichier _pom.xml_ automatiquement créé, et ajouter les dépendances suivantes pour Hadoop, HDFS et Map Reduce:

```xml
  <dependencies>
     <!-- https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common -->
      <dependency>
          <groupId>org.apache.hadoop</groupId>
          <artifactId>hadoop-common</artifactId>
          <version>3.3.5</version>
      </dependency>
      <!-- https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-client-core -->
      <dependency>
          <groupId>org.apache.hadoop</groupId>
          <artifactId>hadoop-mapreduce-client-core</artifactId>
          <version>3.3.5</version>
      </dependency>
      <!-- https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-hdfs -->
      <dependency>
          <groupId>org.apache.hadoop</groupId>
          <artifactId>hadoop-hdfs</artifactId>
          <version>3.3.5</version>
      </dependency>
      <!-- https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-client-common -->
      <dependency>
          <groupId>org.apache.hadoop</groupId>
          <artifactId>hadoop-mapreduce-client-common</artifactId>
          <version>3.3.5</version>
      </dependency>
      <!-- https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-client-jobclient -->
      <dependency>
          <groupId>org.apache.hadoop</groupId>
          <artifactId>hadoop-mapreduce-client-jobclient</artifactId>
          <version>3.3.5</version>
      </dependency>

  </dependencies>
```

  * Créer un package _tp1_ sous le répertoire _src/main/java/hadoop/mapreduce_
  * Créer la classe _TokenizerMapper_, contenant ce code:

```java
  package hadoop.mapreduce.tp1;

  import org.apache.hadoop.io.IntWritable;
  import org.apache.hadoop.io.Text;
  import org.apache.hadoop.mapreduce.Mapper;

  import java.io.IOException;
  import java.util.StringTokenizer;

  public class TokenizerMapper
        extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
  }
```

  * Créer la classe _IntSumReducer_:

```java
package hadoop.mapreduce.tp1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IntSumReducer
        extends Reducer<Text,IntWritable,Text,IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            System.out.println("value: "+val.get());
            sum += val.get();
        }
        System.out.println("--> Sum = "+sum);
        result.set(sum);
        context.write(key, result);
    }
}

```

  * Enfin, créer la classe _WordCount_:

```java
package hadoop.mapreduce.tp1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

```
#### Tester Map Reduce en local
Dans votre projet sur VSCode:

  * Créer un répertoire _input_ sous le répertoire _resources_ de votre projet.
  * Créer un fichier de test: _file.txt_ dans lequel vous insèrerez les deux lignes:
```
  Hello Wordcount!
  Hello Hadoop!
```
  * Nous allons maintenant définir des arguments à la méthode Main: le fichier en entrée sur lequel Map reduce va travailler, et le répertoire en sortie dans lequel le résultat sera stocké. Pour cela:
    * Ouvrir le fichier launch.json de votre projet (Aller à la fenêtre _Run and Debug_, puis cliquer sur _create a launch.json file_).
    * Ajouter la ligne suivante dans la configuration **WordCount**, dont la classe principale est _hadoop.mapreduce.tp1.WordCount_: 
    ```json
    "args": ["wordcount/src/main/resources/input/file.txt","wordcount/src/main/resources/output"]
    ```
    ???note "Arguments"
        Il est à noter que, dans mon cas, le fichier _launch.json_ a été créé sous le répertoire _TP1_, c'est pour cette raison que le chemin des fichiers commence par "wordcount". Si vous créez la configuration directement sous le répertoire _wordcount_, il faudra commencer le chemin par _src_.

  * Sélectionner ensuite, dans la liste des configurations du projet, _WordCount_ comme configuration par défaut:

  ![Default Launch Configuration](img/tp1/launchConfig.png)

  * Lancer le programme. Un répertoire _output_ sera créé dans le répertoire _resources_, contenant notamment un fichier _part-r-00000_, dont le contenu devrait être le suivant:
  
  ```
  Hadoop!	1
  Hello	2
  Wordcount!	1
  ```

#### Lancer Map Reduce sur le cluster
Dans votre projet VSCode:

  * Pour pouvoir encapsuler toutes les dépendances du projet dans le fichier JAR à exporter, ajouter le plugin suivant dans le fichier _pom.xml_ de votre projet:

  
  ```xml hl_lines="10"
  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-assembly-plugin</artifactId>
        <version>3.6.0</version> <!-- Use latest version -->
        <configuration>
          <archive>
            <manifest>
              <mainClass>hadoop.mapreduce.tp1.WordCount</mainClass>
            </manifest>
          </archive>
          <descriptorRefs>
            <descriptorRef>jar-with-dependencies</descriptorRef>
          </descriptorRefs>
        </configuration>
        <executions>
          <execution>
            <id>make-assembly</id> <!-- this is used for inheritance merges -->
            <phase>package</phase> <!-- bind to the packaging phase -->
            <goals>
              <goal>single</goal>
            </goals>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
  ```

  * Aller dans l'Explorer, sous Maven, puis ouvrir le _Lifecycle_ du projet wordcount.
  * Cliquer sur ```package``` pour compiler et packager le projet dans un fichier JAR. Un fichier _wordcount-1.0-SNAPSHOT-jar-with-dependencies.jar_ sera créé sous le répertoire _target_ du projet.
  * Copier le fichier jar créé dans le contenaire namenode. Pour cela:
    - Ouvrir le terminal sur le répertoire du projet _wordcount_. Cela peut être fait avec VSCode en allant au menu _Terminal -> New Terminal_.
    - Taper la commande suivante:
    ```bash
    docker cp target/wordcount-1.0-SNAPSHOT-jar-with-dependencies.jar namenode:/root/wordcount.jar
    ```

  * Revenir au shell du contenaire namenode, et lancer le job map reduce avec cette commande:
  
```bash
  hadoop jar wordcount.jar input output
```

Le Job sera lancé sur le fichier _purchases.txt_ que vous aviez préalablement chargé dans le répertoire _input_ de HDFS. Une fois le Job terminé, un répertoire _output_ sera créé. Si tout se passe bien, vous obtiendrez un affichage ressemblant au suivant:
![Résultat Map Reduce](img/tp1/resultat-mapreduce.png)

En affichant les dernières lignes du fichier généré _output/part-r-00000_, avec ```hdfs dfs -tail output/part-r-00000```, vous obtiendrez l'affichage suivant:

![Affichage Map Reduce](img/tp1/tail.png)

Il vous est possible de monitorer vos Jobs Map Reduce, en allant à la page: ```http://localhost:8088``` (YARN ResourceManager). Vous trouverez votre Job dans la liste des applications comme suit:

![Job MR](img/tp1/job-mr.png)

Il est également possible de voir le comportement du noeud NodeManager, en allant à l'adresse: ```http://localhost:8042```. Vous obtiendrez ce qui suit:

![Job MR](img/tp1/worker-mr.png)

!!! note "Application"
    Écrire un Job Map Reduce permettant, à partir du fichier _purchases_ initial, de déterminer le total des ventes par magasin. Il est à noter que la structure du fichier _purchases_ est de la forme suivante:
    ```
      date   temps   magasin   produit   cout   paiement
    ```
    Veiller à toujours tester votre code en local avant de lancer un job sur le cluster!

## Homework
Vous allez, pour ce cours, réaliser un projet en trinôme ou quadrinôme, qui consiste en la construction d'une architecture Big Data supportant le streaming, le batch processing, et le dashboarding temps réel. Pour la séance prochaine, vous allez commencer par mettre les premières pierres à l'édifice:

* Choisir la source de données sur laquelle vous allez travailler. Je vous invite à consulter les datasets offerts par [Kaggle](https://www.kaggle.com/) par exemple, ou chercher une source de streaming tel que Twitter.
* Réfléchir à l'architecture cible. La pipeline devrait intégrer des traitements en batch, des traitements en streaming et une visualisation.


