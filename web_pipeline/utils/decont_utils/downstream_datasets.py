import json

with open("./utils/decont_utils/data/asdiv.jsonl", "r") as f:
    asdiv_raw = [json.loads(line) for line in f]
    asdiv_tasks = [
        {
            "query": f"{item['body']}",
            "label": f"{item['answer']}",
        }
        for item in asdiv_raw
    ]

with open("./utils/decont_utils/data/gsm8k.jsonl", "r") as f:
    gsm8k_raw = [json.loads(line) for line in f]
    gsm8k_tasks = [
        {
            "query": f"{item['question']}",
            "label": f"{item['cot']} {item['answer']}",
        }
        for item in gsm8k_raw
    ]

with open("./utils/decont_utils/data/math.jsonl", "r") as f:
    math_raw = [json.loads(line) for line in f]
    math_tasks = [
        {
            "query": f"{item['problem']}",
            "label": f"{item['solution']}",
        }
        for item in math_raw
    ]


with open("./utils/decont_utils/data/mathqa.jsonl", "r") as f:
    mathqa_raw = [json.loads(line) for line in f]
    mathqa_tasks = [
        {
            "query": f"{item['problem']} {item['options']}",
            "label": f"{item['rationale']}"[1:-1],  # remove quotes
        }
        for item in mathqa_raw
    ]

with open("./utils/decont_utils/data/mawps.jsonl", "r") as f:
    mawps_raw = [json.loads(line) for line in f]
    mawps_tasks = [
        {
            "query": f"{item['input']}",
            "label": f"{item['target']}",
        }
        for item in mawps_raw
    ]

with open("./utils/decont_utils/data/mmlu_stem.jsonl", "r") as f:
    mmlu_stem_raw = [json.loads(line) for line in f]
    mmlu_stem_tasks = [
        {
            "query": f"{item['question']}",
            "label": "A: "
            + str(item["options"][0])
            + " B: "
            + str(item["options"][1])
            + " C: "
            + str(item["options"][2])
            + " D: "
            + str(item["options"][3])
            + " Answer: "
            + str(item["answer"]),
        }
        for item in mmlu_stem_raw
    ]

with open("./utils/decont_utils/data/ocw.jsonl", "r") as f:
    ocw_raw = [json.loads(line) for line in f]
    ocw_tasks = [
        {
            "query": f"{item['problem']}",
            "label": f"{item['solution']} {item['answer']}",
        }
        for item in ocw_raw
    ]

with open("./utils/decont_utils/data/sat.jsonl", "r") as f:
    sat_raw = [json.loads(line) for line in f]
    sat_tasks = [
        {
            "query": f"{item['question']}",
            "label": f"{item['options']} {item['Answer']}",
        }
        for item in sat_raw
    ]

with open("./utils/decont_utils/data/svamp.jsonl", "r") as f:
    svamp_raw = [json.loads(line) for line in f]
    svamp_tasks = [
        {
            "query": f"{item['Body']} {item['Question']}",
            "label": f"{item['Answer']}",
        }
        for item in svamp_raw
    ]

with open("./utils/decont_utils/data/aime24.jsonl", "r") as f:
    aime24_raw = [json.loads(line) for line in f]
    aime24_tasks = [
        {
            "query": f"{item['problem']}",
            "label": f"{item['solution']}",
        }
        for item in aime24_raw
    ]

with open("./utils/decont_utils/data/aime25.jsonl", "r") as f:
    aime25_raw = [json.loads(line) for line in f]
    aime25_tasks = [
        {
            "query": f"{item['problem']}",
            "label": f"{item['answer']}",
        }
        for item in aime25_raw
    ]

with open("./utils/decont_utils/data/amc.jsonl", "r") as f:
    amc_raw = [json.loads(line) for line in f]
    amc_tasks = [
        {
            "query": f"{item['problem']}",
            "label": f"{item['answer']}",
        }
        for item in amc_raw
    ]

TASK_DATASETS = {
    "asdiv": asdiv_tasks,
    "gsm8k": gsm8k_tasks,
    "math": math_tasks,
    "mathqa": mathqa_tasks,
    "mawps": mawps_tasks,
    "mmlu_stem": mmlu_stem_tasks,
    "ocw": ocw_tasks,
    "sat": sat_tasks,
    "svamp": svamp_tasks,
    "aime24": aime24_tasks,
    "aime25": aime25_tasks,
    "amc": amc_tasks,
}


if __name__ == "__main__":
    for key, value in TASK_DATASETS.items():
        print(key, len(value))
        print(f">>>[Query] {value[0]['query']}")
        print(f">>>[Label] {value[0]['label']}")
        print("-" * 10 + "\n")
