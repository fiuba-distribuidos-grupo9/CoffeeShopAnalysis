def simple_hash(value: str) -> int:
    hash_value = 0
    prime_multiplier = 31
    for char in value:
        char_value = ord(char)
        hash_value = (hash_value * prime_multiplier) + char_value
    return hash_value
