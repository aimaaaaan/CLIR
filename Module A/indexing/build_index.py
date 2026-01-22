import json
import os
import re
from collections import defaultdict, Counter

# ===============================
# CONFIG
# ===============================
OUTPUT_DIR = "index"
os.makedirs(OUTPUT_DIR, exist_ok=True)

CORPORA = {
    "bangla": "C:/Users/X1 Carbon/Documents/1UT/CLIR/Module_A/news_crawler/bangla_corpus.jsonl",
    "english": "C:/Users/X1 Carbon/Documents/1UT/CLIR/Module_A/news_crawler/english_corpus.jsonl"
}

# ===============================
# SIMPLE TOKENIZER
# ===============================
def tokenize(text, language):
    text = text.strip()

    if language == "english":
        text = text.lower()

    # remove punctuation (keep Bangla unicode intact)
    text = re.sub(r"[^\w\s\u0980-\u09FF]", " ", text)

    tokens = text.split()
    return tokens


# ===============================
# BUILD INVERTED INDEX
# ===============================
def build_index(language, corpus_path):
    inverted_index = defaultdict(dict)
    doc_lengths = {}
    total_docs = 0
    skipped_lines = 0

    with open(corpus_path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()

            if not line:
                continue

            try:
                doc = json.loads(line)
            except json.JSONDecodeError as e:
                skipped_lines += 1
                print(f"[WARN] Skipping bad JSON at line {line_no}: {e}")
                continue

            text = doc.get("body", "")
            tokens = tokenize(text, language)

            doc_id = str(total_docs)
            total_docs += 1
            doc_lengths[doc_id] = len(tokens)

            tf = Counter(tokens)
            for term, freq in tf.items():
                inverted_index[term][doc_id] = freq

    stats = {
        "language": language,
        "total_documents": total_docs,
        "skipped_lines": skipped_lines,
        "vocabulary_size": len(inverted_index),
        "average_doc_length": (
            sum(doc_lengths.values()) / total_docs if total_docs > 0 else 0
        )
    }

    return inverted_index, doc_lengths, stats



# ===============================
# RUN FOR BOTH LANGUAGES
# ===============================
for language, path in CORPORA.items():
    print(f"Building index for {language}...")

    inv_index, doc_lengths, stats = build_index(language, path)

    lang_dir = os.path.join(OUTPUT_DIR, language)
    os.makedirs(lang_dir, exist_ok=True)

    with open(os.path.join(lang_dir, "inverted_index.json"), "w", encoding="utf-8") as f:
        json.dump(inv_index, f, ensure_ascii=False, indent=2)

    with open(os.path.join(lang_dir, "doc_lengths.json"), "w", encoding="utf-8") as f:
        json.dump(doc_lengths, f, indent=2)

    with open(os.path.join(lang_dir, "stats.json"), "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)

    print(f"✓ {language} index built")
    print(stats)
