import subprocess

filename = "target/scala-2.12/Scalable2020.jar"
steps = range(1,51)
algorithms = ["LPA","LPA_spark","DLPA","SLPA"]
metrics = "true"
communities = "true"
nodeFile = "data/musae_git_target.csv"
edgeFile = "data/musae_git_edges.csv"
results = "results.csv"
simplify = ["false", "true"]

for alg in algorithms:
    for simplified in simplify:
        for step_num in steps:
            print(alg, simplified, step_num) 
            subprocess.run(
                ["spark-submit", 
                 "--master", "local[*]",
                 filename,
                 "--vertices", nodeFile,
                 "--edges", edgeFile,
                 "--simplify", simplified,
                 "--metrics", metrics,
                 "--algorithm", alg,
                 "--communities", communities,
                 "--steps", str(step_num),
                 "--results", results])
