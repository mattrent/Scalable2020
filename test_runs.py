import subprocess

filename = "/target/scala-2.12/Scalable2020.jar"
steps = range(20, 110, 10)
algorithms = ["SLPA"]
metrics = "true"
communities = "true"
nodeFile = "/data/musae_git_target.csv"
edgeFile = "/data/musae_git_edges.csv"
results = "results_SLPA.csv"
simplify = ["false", "true"]
time="true"
thresholds=["0.01","0.05","0.1"]

for alg in algorithms:
    if(alg!="SLPA"):
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
                     "--time", time,
                     "--steps", str(step_num),
                     "--results", results])
    else:
        for r in thresholds:
            for simplified in simplify:
                for step_num in steps:
                    print("SLPA", simplified, step_num, r)
                    subprocess.run(
                        ["spark-submit",
                            "--master", "local[*]",
                            filename,
                            "--vertices", nodeFile,
                            "--edges", edgeFile,
                            "--simplify", simplified,
                            "--metrics", "false",
                            "--algorithm", "SLPA",
                            "--communities", communities,
                            "--r", r,
                            "--time", time,
                            "--steps", str(step_num),
                            "--results", results])
