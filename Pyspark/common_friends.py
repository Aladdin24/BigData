from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CommonFriends").getOrCreate()

lines = spark.read.text("friends_common.txt")

# Parse lignes
def parse_line(line):
    parts = line.value.strip().split()
    user_id = int(parts[0])
    name = parts[1]
    friends = list(map(int, parts[2].split(','))) if len(parts) > 2 else []
    return (user_id, name, friends)

rdd = lines.rdd.map(parse_line)

# Créer dictionnaire user_id → nom
user_id_name_rdd = rdd.map(lambda x: (x[0], x[1]))
user_id_name_dict = dict(user_id_name_rdd.collect())

# Générer couples
def generate_pairs(user_tuple):
    user_id, name, friends = user_tuple
    pairs = []
    for f in friends:
        pair = tuple(sorted((user_id, f)))
        pairs.append((pair, set(friends)))
    return pairs

pairs_rdd = rdd.flatMap(generate_pairs)

# Grouper par couple
grouped = pairs_rdd.groupByKey()

# Intersecter amis
def find_common_friends(item):
    pair, friends_lists = item
    lists = list(friends_lists)
    if len(lists) > 1:
        common = set.intersection(*lists)
    else:
        common = set()
    return (pair, list(common))

common_friends_rdd = grouped.map(find_common_friends)

# Chercher la paire (1, 2)
target_pair = (min(1, 2), max(1, 2))

result = common_friends_rdd.filter(
    lambda x: x[0] == target_pair
).collect()

if result:
    pair, common_friends_ids = result[0]
    name1 = user_id_name_dict[pair[0]]
    name2 = user_id_name_dict[pair[1]]
    common_friends_names = [user_id_name_dict[fid] for fid in common_friends_ids]
    print(f"{pair[0]}<{name1}>{pair[1]}<{name2}>{common_friends_names}")
else:
    print("Pas d'amis communs pour la paire spécifiée.")
