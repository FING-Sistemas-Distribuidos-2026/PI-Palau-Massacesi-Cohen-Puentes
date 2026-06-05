"""
noise.py
--------
Recibe una frase, le aplica ruido y printea el resultado.

Uso:
    python noise.py "el gato come pescado fresco"
    python noise.py "el gato come pescado fresco" --prob 0.4
    python noise.py "el gato come pescado fresco" --seed 42
"""

import argparse
import random
import unicodedata

from rapidfuzz.distance import Levenshtein
from spellchecker import SpellChecker

# ---------------------------------------------------------------------------
# Vocabulario: diccionario real del español (~60k palabras)
# ---------------------------------------------------------------------------
_spell = SpellChecker(language="es")
VOCABULARY = list(_spell.word_frequency.words())

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def strip_accents(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )


def similar_words(word: str, vocab: list[str], max_dist: int = 2) -> list[str]:
    w = strip_accents(word.lower())
    # Filtrar por longitud similar antes de calcular Levenshtein (evita recorrer todo el vocab)
    candidates = [v for v in vocab if abs(len(v) - len(w)) <= max_dist and v != w]
    return [
        v for v in candidates
        if 1 <= Levenshtein.distance(w, strip_accents(v)) <= max_dist
    ]


def keyboard_typo(word: str) -> str:
    adjacency = {
        "a": "sqzw", "b": "vghn", "c": "xdfv", "d": "erfsc",
        "e": "wsrd", "f": "rtgdc", "g": "fyhve", "h": "gjnb",
        "i": "ujko", "j": "hkni", "k": "jloi", "l": "kñop",
        "m": "nkj",  "n": "bhmj", "o": "ipkl", "p": "oñl",
        "q": "wa",   "r": "etdf", "s": "azxde", "t": "ryfe",
        "u": "yhij", "v": "cfgb", "w": "qase", "x": "zsdc",
        "y": "tugh", "z": "asx",
    }
    if len(word) < 2:
        return word
    idx = random.randint(0, len(word) - 1)
    char = word[idx].lower()
    neighbors = adjacency.get(char, "")
    if not neighbors:
        return word
    replacement = random.choice(neighbors)
    return word[:idx] + replacement + word[idx + 1:]


def corrupt_word(word: str, vocab: list[str]) -> str:
    # Samplear subconjunto para no calcular distancia contra las 60k palabras completas
    sample = random.sample(vocab, min(3000, len(vocab)))
    candidates = similar_words(word, sample, max_dist=2)
    if candidates:
        chosen = random.choice(candidates)
        if word[0].isupper():
            chosen = chosen.capitalize()
        return chosen
    return keyboard_typo(word)


# ---------------------------------------------------------------------------
# Función principal
# ---------------------------------------------------------------------------

def add_noise(phrase: str, prob: float = 0.3) -> str:
    """Recibe una frase y devuelve la versión con ruido."""
    words = phrase.split()
    result = []
    for word in words:
        prefix, suffix, core = "", "", word
        if core and not core[0].isalpha():
            prefix, core = core[0], core[1:]
        if core and not core[-1].isalpha():
            suffix, core = core[-1], core[:-1]

        if core and random.random() < prob:
            core = corrupt_word(core, VOCABULARY)

        result.append(prefix + core + suffix)
    return " ".join(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Agrega ruido a una frase.")
    parser.add_argument("frase", help="Frase de entrada")
    parser.add_argument("--prob", type=float, default=0.3, help="Probabilidad de corrupción por palabra (default: 0.3)")
    parser.add_argument("--seed", type=int, default=None, help="Semilla aleatoria (opcional)")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    print(add_noise(args.frase, prob=args.prob))