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

VOCABULARY = [
    # animales
    "gato", "gata", "pato", "rato", "rata", "toro", "loro", "vaca", "baca",
    "mula", "llama", "cabra", "oveja", "cerdo", "pollo", "conejo", "sapo",
    "lobo", "loba", "zorro", "zorra", "tigre", "liebre", "ciervo", "burro",
    "yegua", "potro", "becerro", "ternero", "ganso", "cisne", "paloma",
    "cuervo", "halcón", "águila", "serpiente", "lagarto", "cocodrilo",
    "delfín", "ballena", "tiburón", "pulpo", "canguro", "koala", "panda",
    "jirafa", "elefante", "gorila", "chimpancé", "rinoceronte", "hipopótamo",

    # comida y bebida
    "come", "comen", "comer", "comió", "pan", "pez", "pescado", "carne",
    "torta", "tarta", "pasta", "sopa", "sal", "salsa", "queso", "leche",
    "huevo", "fruta", "mango", "papa", "tapa", "copa", "arroz", "maíz",
    "trigo", "harina", "azúcar", "miel", "manteca", "aceite", "vinagre",
    "limón", "naranja", "manzana", "pera", "uva", "fresa", "cereza",
    "sandía", "melón", "durazno", "ciruela", "higo", "dátil", "coco",
    "piña", "banana", "kiwi", "tomate", "cebolla", "zanahoria", "lechuga",
    "espinaca", "pepino", "pimiento", "ajo", "jengibre", "canela", "pimienta",
    "café", "mate", "vino", "cerveza", "agua", "jugo", "té", "leche",
    "helado", "torta", "galleta", "chocolate", "caramelo", "mermelada",

    # acciones / verbos
    "corre", "salta", "mira", "tira", "gira", "habla", "sube", "bebe",
    "debe", "viene", "toma", "loma", "doma", "canta", "llora", "ríe",
    "duerme", "sueña", "piensa", "busca", "encuentra", "pierde", "gana",
    "vende", "compra", "lleva", "trae", "pide", "da", "recibe", "abre",
    "cierra", "rompe", "arregla", "limpia", "sucia", "pinta", "dibuja",
    "escribe", "lee", "estudia", "enseña", "aprende", "juega", "trabaja",
    "descansa", "viaja", "regresa", "sale", "entra", "sube", "baja",
    "nada", "vuela", "camina", "tropieza", "cae", "levanta", "empuja",
    "jala", "corta", "cose", "teje", "cocina", "hornea", "fríe", "hierve",

    # descripciones / adjetivos
    "rojo", "verde", "azul", "amarillo", "negro", "blanco", "gris",
    "violeta", "naranja", "rosa", "marrón", "celeste", "dorado", "plateado",
    "grande", "pequeño", "alto", "bajo", "gordo", "flaco", "rápido", "lento",
    "fuerte", "débil", "duro", "blando", "caliente", "frío", "seco", "mojado",
    "limpio", "sucio", "nuevo", "viejo", "joven", "anciano", "bonito", "feo",
    "rico", "pobre", "feliz", "triste", "enojado", "asustado", "valiente",
    "tímido", "listo", "tonto", "sabio", "loco", "cuerdo", "sano", "enfermo",
    "cansado", "descansado", "hambriento", "satisfecho", "sediento", "borracho",
    "dormido", "despierto", "vivo", "muerto", "libre", "preso", "solo", "acompañado",

    # lugares
    "casa", "caza", "mesa", "rosa", "calle", "valle", "ciudad", "pueblo",
    "campo", "bosque", "selva", "desierto", "montaña", "río", "lago",
    "mar", "océano", "isla", "playa", "costa", "puerto", "puente", "ruta",
    "camino", "sendero", "parque", "jardín", "plaza", "mercado", "tienda",
    "hospital", "escuela", "iglesia", "castillo", "palacio", "torre",
    "puerta", "ventana", "techo", "suelo", "pared", "escalera", "balcón",
    "cocina", "baño", "dormitorio", "sala", "patio", "garage", "sótano",

    # objetos cotidianos
    "mesa", "silla", "cama", "sofá", "lámpara", "espejo", "cuadro",
    "libro", "lápiz", "bolígrafo", "papel", "tijera", "aguja", "hilo",
    "taza", "plato", "vaso", "cuchillo", "tenedor", "cuchara", "olla",
    "sartén", "heladera", "horno", "reloj", "teléfono", "radio", "televisor",
    "mochila", "bolso", "billetera", "llave", "candado", "cadena", "cuerda",
    "pelota", "bicicleta", "auto", "camión", "barco", "avión", "cohete",
    "paraguas", "abrigo", "camisa", "pantalón", "zapato", "sombrero", "guante",

    # naturaleza / clima
    "sol", "luna", "estrella", "nube", "lluvia", "nieve", "viento", "tormenta",
    "rayo", "trueno", "niebla", "arco iris", "tierra", "piedra", "arena",
    "fuego", "humo", "lava", "hielo", "flor", "árbol", "raíz", "hoja",
    "rama", "semilla", "fruto", "espina", "musgo", "hongo", "hierba",

    # personas / roles
    "hombre", "mujer", "niño", "niña", "bebé", "abuelo", "abuela",
    "padre", "madre", "hijo", "hija", "hermano", "hermana", "primo",
    "amigo", "enemigo", "vecino", "maestro", "médico", "policía",
    "soldado", "rey", "reina", "príncipe", "princesa", "bruja", "mago",
    "héroe", "villano", "fantasma", "robot", "gigante", "enano",

    # conectores / pronombres / artículos
    "el", "la", "los", "las", "un", "una", "unos", "unas",
    "en", "con", "sin", "por", "para", "sobre", "bajo", "entre",
    "que", "como", "cuando", "donde", "quien", "cual",
    "se", "te", "me", "le", "nos", "les", "yo", "tu", "él", "ella",
    "todo", "toda", "todos", "todas", "algo", "nadie", "nada", "cada",
    "muy", "más", "menos", "tan", "tanto", "poco", "mucho", "bastante",
    "ya", "aún", "aquí", "allí", "ahora", "antes", "después", "siempre",
    "nunca", "quizás", "tal", "vez", "también", "tampoco", "sino", "pero",
]

VOCABULARY = list({w.lower() for w in VOCABULARY})

# Helpers

def strip_accents(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )


def similar_words(word: str, vocab: list[str], max_dist: int = 2) -> list[str]:
    w = strip_accents(word.lower())
    return [
        v for v in vocab
        if v != word.lower()
        and 1 <= Levenshtein.distance(w, strip_accents(v)) <= max_dist
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
    candidates = similar_words(word, vocab, max_dist=2)
    if candidates:
        chosen = random.choice(candidates)
        if word[0].isupper():
            chosen = chosen.capitalize()
        return chosen
    return keyboard_typo(word)



# Función principal

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