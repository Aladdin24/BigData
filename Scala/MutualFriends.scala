// Charger le fichier
val data = sc.textFile("soc-LiveJournal1Adj.txt")

// Nettoyer les lignes vides ou mal formées
val data1 = data.map(x => x.split("\t"))
                .filter(li => li.size == 2)

// Fonction pour générer les paires (userA, userB)
def pairs(str: Array[String]) = {
    val users = str(1).split(",")
    val user = str(0)
    val n = users.length
    for (i <- 0 until n) yield {
        val pair =
            if (user < users(i))
                (user, users(i))
            else
                (users(i), user)
        (pair, users)
    }
}

// Créer toutes les paires et trouver intersection des amis
val pairCounts = data1
    .flatMap(pairs)
    .reduceByKey((friends1, friends2) => friends1.intersect(friends2))

// Transformer le résultat au format demandé
val p1 = pairCounts.map {
    case ((userA, userB), mutualFriends) =>
        userA + "\t" + userB + "\t" + mutualFriends.mkString(",")
}

// Sauvegarder le fichier final
p1.saveAsTextFile("output")

// Extraction des mutual friends pour des paires spécifiques
var ans = ""

// Extrait la paire (0,4)
val p2 = p1.map(x => x.split("\t"))
           .filter(x => x.size == 3)
           .filter(x => x(0) == "0" && x(1) == "4")
           .flatMap(x => x(2).split(","))
           .collect()

ans = ans + "0" + "\t" + "4" + "\t" + p2.mkString(",") + "\n"

// Extrait la paire (20,22939)
val p3 = p1.map(x => x.split("\t"))
           .filter(x => x.size == 3)
           .filter(x => x(0) == "20" && x(1) == "22939")
           .flatMap(x => x(2).split(","))
           .collect()

ans = ans + "20" + "\t" + "22939" + "\t" + p3.mkString(",") + "\n"

// Extrait la paire (1,29826)
val p4 = p1.map(x => x.split("\t"))
           .filter(x => x.size == 3)
           .filter(x => x(0) == "1" && x(1) == "29826")
           .flatMap(x => x(2).split(","))
           .collect()

ans = ans + "1" + "\t" + "29826" + "\t" + p4.mkString(",") + "\n"

// Extrait la paire (19272,6222)
val p5 = p1.map(x => x.split("\t"))
           .filter(x => x.size == 3)
           .filter(x => x(0) == "19272" && x(1) == "6222")
           .flatMap(x => x(2).split(","))
           .collect()

ans = ans + "19272" + "\t" + "6222" + "\t" + p5.mkString(",") + "\n"

// Extrait la paire (28041,28056)
val p6 = p1.map(x => x.split("\t"))
           .filter(x => x.size == 3)
           .filter(x => x(0) == "28041" && x(1) == "28056")
           .flatMap(x => x(2).split(","))
           .collect()

ans = ans + "28041" + "\t" + "28056" + "\t" + p6.mkString(",") + "\n"

// Sauvegarder le résultat spécifique
val answer = sc.parallelize(Seq(ans))
answer.saveAsTextFile("output1")
