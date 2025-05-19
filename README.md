# Readme - Sequence similarity join 

## Structure du Répertoire

1. **all_similarity_joins/**
   - Ce dossier contient le code source Java pour le projet Hadoop et un environnement de test local sous Intellij.
      - similarity-core/utils/SetupTools.java : les paramètres de l'algorithme,
      -  similarity-core/verifier/* : les codes des distances,
      - similarity-core/writables/lsh/SlicedQGramsLSH.java : la classe responsable du hachage LSH sur les séquences,
      - similarity-core/writables/estimator/EditSketching.java : la classe responsable du sketching des séquences,
      - similarity-jobs/jobs/mrsf/* : les jobs de l'algorithmes,
      - similarity-jobs/mrjobs/mrsf/* : les algorithmes correspondant aux jobs,
      - similarity-tokenizer : les jobs pour supprimer les duplicas et mélanger l'entrée,

2. **dataset/**
   - Ce dossier contient trois jeux de données et leurs résultats dans l'ordre du papier:
      - **synthetic_sizes** : séquence de taille de 25 à 10000,
      - **synthetic_noise** : ajout de bruits sur des séquence de taille 1000,
      - **metaclust** : [https://metaclust.mmseqs.org/current_release/metaclust_all.gz](https://metaclust.mmseqs.org/current_release/)

3. **scripts/**
   - Ce dossier contient plusieurs fichiers :
      - **conf_mrfv.xml** : la configuration XML des paramètres de l'algorithme MRSF,
      - **conf_tokenizer.xml** : la configuration XML des paramètres du tokenizer,
      - **start_join_mrfv.sh** : le script bash exécutant les phases MapReduce de l'algorithme MRFS,
      - **tokenizer.sh** : supprime les enregistrements dupliqués et mélange les enregistrements,
      - **update_input.sh** : copie les entrées sur HDFS,
      - **update_seed.sh** : copie les graines aléatoires sur l'HDFS, que l'on peut retrouver dans le dossier `seeds` pour des raisons de reproductibilité.

## Configuration du Projet

1. **Compilation du Code Java**
   - Accédez au dossier `all_similarity_joins/` et compilez le code Java avec Maven.

   ```bash
   mvn clean package
   ```

2. **Exécution sous Hadoop**
   ```bash
   bash update_input.sh $INPUT 
   bash update_seed.sh
   bash tokenizer.sh $NB_FILES 
   bash start_join_mrfv.sh $NB_REDUCER_HISTOGRAM $NB_REDUCER_FILTER $NB_SPLITS $NB_REDUCER_JOIN
   ```
