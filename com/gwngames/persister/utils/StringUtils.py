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
        words = text.split()
        for i, word in enumerate(words):
            next_index = current_index + len(word)
            if current_index <= fifth_index < next_index:
                # Check if the word is shorter than 2 characters
                if len(word) < 2:
                    # Return the next word if it exists
                    return words[i + 1] if i + 1 < len(words) else None
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

print(StringUtils.first_after_fifth("avalanche: a pytorch library for deep continual learning"))