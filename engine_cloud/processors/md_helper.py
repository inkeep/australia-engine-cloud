import re

import markdownify
from bs4 import BeautifulSoup
from bs4.element import Comment


def add_spaces_between_divs(html_content, parser: str):
    soup = BeautifulSoup(html_content, parser)

    for div in soup.find_all(True):
        # https://vercel.com/docs/rest-api/endpoints/webhooks
        if (
            div
            and div.name == "div"
            and div.parent
            and div.parent.name == "code"
            and div.parent.parent
            and div.parent.parent.name == "pre"
        ):
            if div.next_sibling:
                div.insert_after("\n")
        else:
            if (
                "gap-x" in " ".join(div.parent.get("class", []) or [])
            ) and div.name == "span":
                if div.next_sibling:
                    div.insert_after(" ")
                # Add a space before each div
                if div.previous_sibling:
                    div.insert_before(" ")
            elif div.name == "span":
                continue

            # Add a space after each div
            if div.next_sibling:
                div.insert_after(" ")
            # Add a space before each div
            if div.previous_sibling:
                div.insert_before(" ")

    return str(soup)


def remove_comments_and_convert_to_soup(html, parser="lxml"):
    soup = BeautifulSoup(html, parser)
    comments = soup.findAll(string=lambda text: isinstance(text, Comment))
    for comment in comments:
        comment.extract()  # This removes the comment from the soup
    return soup  # After all comments are removed


class CustomMarkdownConverter(markdownify.MarkdownConverter):
    def __init__(self, **options):
        super().__init__(**options)

    def convert(self, html):
        try:
            html = add_spaces_between_divs(html, "lxml")
            soup = remove_comments_and_convert_to_soup(html, "lxml")
            return self.convert_soup(soup)
        except Exception as e:
            html = add_spaces_between_divs(html, "html.parser")
            html = remove_comments_and_convert_to_soup(html, "html.parser")
            return self.convert_soup(soup)

    # def convert_table(self, el, text, convert_as_inline):
    #     return "\n\n" + text + "\n"

    def convert_td(self, el, text, convert_as_inline):
        return " " + text.strip() + " |"

    # def convert_th(self, el, text, convert_as_inline):
    #     return " " + text + " |"

    def convert_tr(self, el, text, convert_as_inline):
        cells = el.find_all(["td", "th"])
        is_headrow = all([cell.name == "th" for cell in cells])

        el_previous_sibling = el.find_previous_sibling()
        el_parent_previous_sibling = (
            el.parent.find_previous_sibling() if el.parent else None
        )

        overline = ""
        underline = ""
        if is_headrow and not el_previous_sibling:
            # first row and is headline: print headline underline
            underline += "| " + " | ".join(["---"] * len(cells)) + " |" + "\n"
        elif not el_previous_sibling and (
            el.parent.name == "table"
            or (el.parent.name == "tbody" and not el_parent_previous_sibling)
        ):
            # first row, not headline, and:
            # - the parent is table or
            # - the parent is tbody at the beginning of a table.
            # print empty headline above this row
            overline += "| " + " | ".join([""] * len(cells)) + " |" + "\n"
            overline += "| " + " | ".join(["---"] * len(cells)) + " |" + "\n"
        else:
            pass

        return overline + "|" + text + "\n" + underline

    def convert_img(self, el, text, convert_as_inline):
        alt = el.attrs.get("alt", None) or ""
        src = ""
        title = el.attrs.get("title", None) or ""
        title_part = ' "%s"' % title.replace('"', r"\"") if title else ""
        if (
            convert_as_inline
            and el.parent.name not in self.options["keep_inline_images_in"]
        ):
            return alt

        return "![%s](%s%s)" % (alt, src, title_part)


def custom_markdownify(html, **options):
    return CustomMarkdownConverter(**options).convert(html)


CODE_LANGUAGE_MAPPING = {
    "ts": "typescript",
    "js": "javascript",
}


def extract_code_language_from_class(language_class):
    # print("language_class", language_class)
    if language_class and isinstance(language_class, str):
        if "language-" in language_class:
            return language_class.split("-")[1]
        if "lang-" in language_class:
            return language_class.split("-")[1]
    elif language_class and isinstance(language_class, list):
        for item in language_class:
            language = extract_code_language_from_class(item)
            if language:
                return language
    return None


def code_language_callback(el, depth=0):
    if depth > 3:  # Stop if depth is greater than 3
        return None

    # Check the current element
    code_language = extract_code_language_from_class(el.get("class"))
    if code_language:
        # print("code_language", code_language)
        return code_language

    # If no language found, check up to two parents
    if depth < 2:  # Only check parents if depth is less than 2
        parent = el.parent
        if parent:
            code_language = code_language_callback(parent, depth + 1)
            if code_language:
                # print("code_language", code_language)
                return code_language

    # If still no language found, check for a specific child (only at depth 0)
    if depth == 0:
        # print([child.name for child in el.children])
        code_children = [child for child in el.children if child.name == "code"]
        if len(code_children) == 1:
            code_child = code_children[0]
            if code_child:
                code_language = code_language_callback(code_child, depth + 3)
                if code_language:
                    # print("code_language", code_language)
                    return code_language

    return None


def convert_html_to_md(html: str) -> str:
    html = html.strip()
    md = custom_markdownify(
        html,
        heading_style="ATX",
        escape_asterisks=False,
        escape_underscores=False,
        code_language_callback=code_language_callback,
    )
    # md = re.sub("\n{3,}", "\n\n", md)
    md = re.sub(r"(\n\s*){3,}", "\n\n", md)
    md = md.strip()
    return md
