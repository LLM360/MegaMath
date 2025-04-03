import os
import re
import unicodedata

import fasttext
from fasttext.FastText import _FastText
from nltk.tokenize import wordpunct_tokenize

LABEL_PREFIX = "__label__"
LABEL_MATH = f"{LABEL_PREFIX}MATH"
LABEL_NON_MATH = f"{LABEL_PREFIX}NON_MATH"


def normalization(text):
    tokens = wordpunct_tokenize(text)

    processed_tokens = []
    for token in tokens:
        token = token.lower()

        if token.isdigit():
            processed_tokens.append("<NUM>")
        elif len(token) <= 100:
            processed_tokens.append(token)

    preprocessed_text = " ".join(processed_tokens)

    preprocessed_text = re.sub(r"[\n\r]+", " ", preprocessed_text)
    preprocessed_text = re.sub(r"[-_]+", " ", preprocessed_text)
    preprocessed_text = re.sub(r"[^a-zA-Z0-9\s<NUM>]", "", preprocessed_text)
    preprocessed_text = re.sub(r"\s+", " ", preprocessed_text).strip()

    return preprocessed_text


def preprocess_for_fasttext(text):
    if isinstance(text, bytes):
        text = text.decode("utf-8")

    text = unicodedata.normalize("NFKC", text)

    text = re.sub(r"\s", " ", text)

    text = text.replace("\n", " <EOS> ")

    text = re.sub(r"\s+", " ", text)

    text = normalization(text)

    MAX_LINE_SIZE = 1024
    lines = text.split("<EOS>")
    processed_lines = []
    for line in lines:
        tokens = line.split()
        if len(tokens) > MAX_LINE_SIZE:
            processed_lines.extend(
                [
                    " ".join(tokens[i : i + MAX_LINE_SIZE])
                    for i in range(0, len(tokens), MAX_LINE_SIZE)
                ]
            )
        else:
            processed_lines.append(line)

    text = " <EOS> ".join(processed_lines)

    return text.strip()


class MathFastTextClassifier:
    name = "+ - * ÷ Math FastText Recall"
    _requires_dependencies = [("fasttext", "fasttext-wheel"), "fasteners"]

    def __init__(
        self,
        model_path: str | None = None,
        math_threshold: float = 0.95,
        math_class_name: str = None,
    ):
        assert model_path is not None, "please specify the model path"
        self.model_path = model_path
        self.model = fasttext.load_model(model_path)
        self.math_threshold = math_threshold
        self.math_class_name = math_class_name

    def __getstate__(self):
        """Custom pickling method to avoid pickling the FastText model directly."""
        state = self.__dict__.copy()
        # Remove the model from the state to avoid pickling issues
        state["model"] = None
        return state

    def __setstate__(self, state):
        """Custom unpickling method to reload the FastText model."""
        self.__dict__.update(state)
        # Reload the model after unpickling
        self.model = fasttext.load_model(self.model_path)

    def predict(self, text: str) -> bool:
        class_tuples, prob_tuples = self.model.predict(
            preprocess_for_fasttext(
                text,
            ),
            k=-1,
        )

        assert len(class_tuples) == len(prob_tuples)
        math_score = 0
        for class_name, prob in zip(class_tuples, prob_tuples):
            if class_name == self.math_class_name:
                math_score = prob
                break

        # print(f"math score: {math_score}")
        return math_score >= self.math_threshold, math_score
