from typing import List

class StringUtils:
    class SemicolonFoundException(Exception):
        pass

    @staticmethod
    def is_first_word_short(text):
        words = text.split()
        if not words:
            return False
        return len(words[0]) <= 1

    @staticmethod
    def first_after_fifth(text):
        if not text:
            return None

        total_chars = len(text.strip())
        fifth_index = total_chars // 5

        current_index = 0
        for word in text.split():
            next_index = current_index + len(word)
            if current_index <= fifth_index < next_index:
                return word
            current_index = next_index + 1

        return None

    @staticmethod
    def process_string(input_string: str) -> List[str]:
        if ';' in input_string:
            raise StringUtils.SemicolonFoundException("Input string contains a semicolon")
        else:
            # Split the string by commas
            result = input_string.split(',')
            return result

    @staticmethod
    def sanitize_string(input_string):
        trimmed_string = input_string.strip()
        invalid_chars = '<>:"/\\|?*'
        sanitized_string = ''.join('' if c in invalid_chars else c for c in trimmed_string)

        return sanitized_string

