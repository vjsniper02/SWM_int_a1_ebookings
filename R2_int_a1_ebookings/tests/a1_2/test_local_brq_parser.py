from os import path


class TestBRQParser:

    def test_brq_parser(self):
        file_path = path.join(
            path.dirname(__file__),
            "../brq_test_files/BrJohnson@Seven.com.au_SEVNET-2024-Request-00030290-STARCO.brq",
        )

        with open(file_path) as f:
            from functions.BRQParser import BRQParser

            content = f.read()
            print(content)
            parser = BRQParser(content)
            parser.parse()
            if parser.has_error():
                print("ERROR")
                raise parser.get_error()
