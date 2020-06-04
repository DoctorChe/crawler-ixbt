import hashlib

# Список хэш функций
HASH_FUNCTIONS = [
    lambda string: hashlib.blake2s(string.encode(encoding='UTF-8')),
    lambda string: hashlib.sha3_512(string.encode(encoding='UTF-8')),
    lambda string: hashlib.sha3_256(string.encode(encoding='UTF-8')),
    lambda string: hashlib.shake_128(string.encode(encoding='UTF-8')),
    lambda string: hashlib.sha224(string.encode(encoding='UTF-8')),
    lambda string: hashlib.sha3_384(string.encode(encoding='UTF-8')),
    lambda string: hashlib.sha3_224(string.encode(encoding='UTF-8')),
    lambda string: hashlib.md5(string.encode(encoding='UTF-8')),
    lambda string: hashlib.sha384(string.encode(encoding='UTF-8')),
    lambda string: hashlib.sha1(string.encode(encoding='UTF-8')),
    lambda string: hashlib.shake_256(string.encode(encoding='UTF-8')),
    lambda string: hashlib.sha512(string.encode(encoding='UTF-8')),
    lambda string: hashlib.sha256(string.encode(encoding='UTF-8')),
    lambda string: hashlib.blake2b(string.encode(encoding='UTF-8')),
]


class Analyzer:
    @staticmethod
    def shingles(words: list, shingles_len=3):
        """
        Алгоритм разделения текста на шинглы
        Получаем слова, составляющие шинглы и собирем шинглы
        """
        return [''.join(words[i: i + shingles_len]) for i in range(len(words) - shingles_len + 1)]

    @staticmethod
    def min_hash(shingles_list1: list, shingles_list2: list):
        """
        Алгоритм хэш-функций
        """
        hash_values1 = set([min(list(map(lambda func: hash(shingle) % 100000, HASH_FUNCTIONS)))
                            for shingle in shingles_list1])
        hash_values2 = set([min(list(map(lambda func: hash(shingle) % 100000, HASH_FUNCTIONS)))
                            for shingle in shingles_list2])
        return len(hash_values1.intersection(hash_values2)) / len(hash_values1.union(hash_values2))

    def shingles_min_hash(self, words1, words2):
        return self.min_hash(self.shingles(words1), self.shingles(words2))
