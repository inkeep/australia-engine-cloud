import codecs
import json
import re
import typing as T
from datetime import datetime

from engine_cloud.utils.helpers import get_num_tokens, get_content_hash


MAX_TOKENS = 6144
MIN_TOKENS = 512


class PageNode:
    def __init__(
        self,
        url: str,
        content: str,
        title: str,
        meta: T.List[dict],
        is_root: bool = False,
        date_published: str = "",
        root_title: str = "",
    ) -> None:
        self.content: str = content
        self.url: str = url
        self.title: str = title
        self.root_title: str = root_title
        self.children: T.List[PageNode] = []
        self.prev_sibiling: PageNode = None
        self.next_sibiling: PageNode = None
        self.parent: PageNode = None
        self.coalesced_nodes = []
        self.is_root = is_root
        self.meta = meta
        self.date_published = date_published

    def add_child(self, child) -> None:
        self.children.append(child)

    def set_parent(self, parent) -> None:
        self.parent = parent

    def set_prev_sibiling(self, prev_sibiling) -> None:
        self.prev_sibiling = prev_sibiling

    def set_next_sibiling(self, next_sibiling) -> None:
        self.next_sibiling = next_sibiling

    def to_record_dict(self) -> dict:
        return {
            # TODO: 16000 should be a parameter
            "content": self.content if get_num_tokens(self.content) > 64000 else "",
            "url": self.url,
            "id": get_content_hash(self.url),
        }

    def set_content(self, content) -> None:
        self.content = content

    def get_total_token_count(self):
        count = 0
        for child in self.children:
            count += child.get_total_token_count()
        count += get_num_tokens(self.content)
        return count

    def reconstruct_content(self) -> str:
        content = self.content
        if not self.children:
            return self.content
        for child in self.children:
            content = content + "\n" + child.reconstruct_content()
        return content.strip()


def identify_header(line):
    # Matches headers in the pattern `# Header`
    match = re.match(r"^(#+) ", line)
    if match:
        header_level = len(match.group(1))
        return header_level
    return None


def extract_url_from_header(line):
    pattern = r"^#+ \[.*?\]\((https?://[^\)]+)\)"

    # Find matches
    match = re.search(pattern, line)
    if match:
        url = match.group(1)
        return url
    return None


def extract_title_from_header(header):
    pattern = r"^#+ +(?:\!?\[.*?\]\(https?://[^\)]+\) )?(.+)"
    singlestore_pattern = r"\*\*(.*?)\*\*"
    print(header)
    singlestore_match = re.search(singlestore_pattern, header)
    match = re.search(pattern, header)
    if singlestore_match:
        return singlestore_match.group(1).replace(r"\.", ".")

    if match:
        title = match.group(1)
        next_match = re.search(r"\[(.*?)\]\(https://.*\)", title)
        if not next_match:
            next_match = re.search(r"\[\`? (.*?)\(?\)? \`?\]*\(https://.*\)", title)
        title = re.sub(r"\[(.*)\]\(https://.*\)", "", title)
        answer = title + next_match.group(1) if next_match else title
        answer = answer.split("  ")[0]
        answer = answer.replace(r"\.", ".")

        return answer.strip().lstrip()
    return ""


def set_siblings(current_node: PageNode, prev_node: PageNode):
    current_node.set_prev_sibiling(prev_node)
    prev_node.set_next_sibiling(current_node)
    current_node.set_parent(prev_node.parent)
    current_node.parent.add_child(current_node)


def find_parent_node(level_nodes, current_level):
    for i in range(current_level - 1, 0, -1):
        if len(level_nodes[i]) > 0:
            return level_nodes[i][-1]
    return level_nodes[0][0]


def find_header_order(content):
    lines = content.split("\n")
    headers = []
    for line in lines:
        header_level = identify_header(line)
        if header_level and header_level not in headers:
            headers.append(header_level)
    headers.sort()
    return headers


def get_page_graph_from_content(
    record,
    content,
    max_depth=2,
    min_tokens=MIN_TOKENS,
    coalesce=True,
):
    base_url = record["url"]
    base_title = record["title"]
    attributes = record["attributes"]
    meta = attributes["meta"] if "meta" in attributes else []

    if "date_published" in attributes:
        date_published = attributes["date_published"]
    else:
        date_published = ""

    lines = content.split("\n")

    root_node = PageNode(
        base_url, "", base_title, meta, is_root=True, date_published=date_published
    )

    prev_level = None
    prev_node = None
    current_level = 0
    current_content = []
    current_node = root_node
    level_nodes = {i: [] for i in range(1, max_depth + 1)}
    level_nodes[0] = [root_node]
    header_order = find_header_order(content)
    if max_depth > len(header_order):
        print("depth not compatible with record content, truncating")
        max_depth = len(header_order)
    for line in lines:
        header_level = identify_header(line)
        if header_level and header_level <= header_order[max_depth - 1]:
            prev_node = current_node
            prev_level = current_level
            current_level = header_order.index(header_level) + 1
            extracted_url = extract_url_from_header(line)
            current_node = PageNode(
                base_url,
                "",
                extract_title_from_header(line),
                meta,
                date_published=prev_node.date_published,
                root_title=base_title,
            )
            if level_nodes.get(current_level) is None:
                level_nodes[current_level] = [current_node]
            else:
                level_nodes[current_level].append(current_node)
            # TODO: need to handle when there are multiple level 1 headers
            if prev_level > current_level:
                parent = find_parent_node(level_nodes, current_level)
                if parent:
                    current_node.set_parent(parent)
                    parent.add_child(current_node)
            elif prev_level < current_level:
                current_node.set_parent(prev_node)
                prev_node.add_child(current_node)
            elif prev_level == current_level:
                set_siblings(current_node, prev_node)

            prev_node.set_content("\n".join(current_content))
            current_content = []

        current_content.append(line)
        current_node.set_content("\n".join(current_content))

    if coalesce:
        coalesce_small_subtrees(root_node, min_tokens)
    return root_node


def coalesce_small_subtrees(node: PageNode, min_tokens: int):
    if not node.children:
        return get_num_tokens(node.content)

    initial_children = list(node.children)
    current_token_count = 0
    coalesced_children = []
    prev_node = None
    # first try to combine the children with each other as long as they have no children
    for child in initial_children:
        token_count = coalesce_small_subtrees(child, min_tokens)
        if not child.children:
            if prev_node and current_token_count + token_count <= min_tokens:
                prev_node.content += "\n" + child.content
                prev_node.coalesced_nodes.append(child)
                current_token_count += token_count
            elif prev_node:
                coalesced_children.append(prev_node)
                prev_node = child
                current_token_count = token_count
            else:
                prev_node = child
                current_token_count = token_count
        elif prev_node:
            coalesced_children.append(prev_node)
            coalesced_children.append(child)
            prev_node = None
        else:
            coalesced_children.append(child)

    if prev_node:
        coalesced_children.append(prev_node)

    node.children = coalesced_children

    # next try to combine the children in order with the parent as long as it is not the root
    if node.is_root:
        return
    coalesced_prev = True
    current_token_count = get_num_tokens(node.content)
    current_children = list(node.children)
    for child in current_children:
        token_count = child.get_total_token_count()
        if coalesced_prev and current_token_count + token_count <= min_tokens:
            node.content += "\n" + child.content
            node.coalesced_nodes.append(child)

            node.children.remove(child)
        else:
            coalesced_prev = False
        current_token_count += token_count

    return current_token_count


def create_graph_record(
    current_node: PageNode,
    children_list: T.List[dict],
    next_sibiling: dict,
    prev_sibiling: dict,
    content: str,
    content_full: str,
    html: str,
) -> dict:
    return {
        "record_id": get_content_hash(content),
        "title": current_node.root_title.strip().replace("///#", "#"),
        "url": current_node.url,
        "content": content.replace("///#", "#"),
        "format": "md",
        "attributes": {
            "num_tokens": get_num_tokens(content),
            "date_published": current_node.date_published,
            "record_type": "docs" if current_node.url.find("/docs") != -1 else "site",
            "hash_html": get_content_hash(html),
            "index_version": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "meta": current_node.meta,
            "sitemap_tags": json.dumps(""),
        },
        "extra_attributes": {
            "parent": (
                json.dumps(current_node.parent.to_record_dict())
                if current_node.parent
                else None
            ),
            "children": json.dumps(children_list),
            "next_sibling": json.dumps(next_sibiling),
            "prev_sibling": json.dumps(prev_sibiling),
            "full content": (
                content_full if get_num_tokens(content_full) < 16000 else ""
            ),
            "top_level_headings": json.dumps(get_top_level_headings(content_full)),
        },
    }


def convert_record(es_record):
    return {
        "record_id": es_record["record_id"],
        "content": es_record["content"],
        "title": es_record["title"],
        "url": es_record["url"],
        "format": "md",
        "attributes": es_record["attributes"],
        "extra_attributes": es_record["extra_attributes"],
    }


def split_by_paragraph(md_content, max_tokens=MAX_TOKENS):
    paragraphs = md_content.split(
        "\n\n"
    )  # assuming paragraphs are separated by two newlines
    current_section = []
    sections = []

    for paragraph in paragraphs:
        if get_num_tokens("\n\n".join(current_section + [paragraph])) <= max_tokens:
            current_section.append(paragraph)
        else:
            sections.append("\n\n".join(current_section))
            current_section = [paragraph]

    if current_section:
        sections.append("\n\n".join(current_section))

    return sections


def create_records_from_page_graph(
    root: PageNode, html: str = "", max_tokens=MAX_TOKENS
):
    records = []
    content_full = root.reconstruct_content()
    for root_child in root.children:
        queue = [root_child]
        while len(queue) > 0:
            current_node = queue.pop(0)
            if not current_node:
                print("current node is none")
                continue
            children_list = []
            child_token_count = 0
            for child in current_node.children:
                child_token_count += get_num_tokens(child.content)
                children_list.append(child.to_record_dict())

            # if the total child content is greater than 16k, we don't want to store it
            if child_token_count > 16000:
                for child_dict in children_list:
                    child_dict["content"] = ""

            prev_sibiling = (
                current_node.prev_sibiling.to_record_dict()
                if current_node.prev_sibiling
                else {"content": ""}
            )

            next_sibiling = (
                current_node.next_sibiling.to_record_dict()
                if current_node.next_sibiling
                else {"content": ""}
            )

            if (
                get_num_tokens(prev_sibiling["content"] + next_sibiling["content"])
                > max_tokens
            ):
                prev_sibiling["content"] = ""
                next_sibiling["content"] = ""

            if get_num_tokens(current_node.content) > max_tokens:
                sections = split_by_paragraph(current_node.content)
                for section in sections:
                    records.append(
                        create_graph_record(
                            current_node,
                            children_list,
                            next_sibiling,
                            prev_sibiling,
                            section,
                            content_full,
                            html,
                        )
                    )
            else:
                "adding node record"
                records.append(
                    create_graph_record(
                        current_node,
                        children_list,
                        next_sibiling,
                        prev_sibiling,
                        current_node.content,
                        content_full,
                        html,
                    )
                )
            queue.extend(current_node.children)

    return records


def create_graphs_from_index(
    index_name: str,
    max_depth=2,
    coalesce=True,
    records=[],
):
    skip_string = """Knock Docs \n\n---\n\n * [ Home ](https://docs.knock.app/)\n * [ Building in-app UI ](https://docs.knock.app/in-app-ui/overview)\n * [ Android (Kotlin) ](https://docs.knock.app/in-app-ui/android/overview)\n * [ Overview ](https://docs.knock.app/in-app-ui/android/overview)\n\n---\n\n"""

    graphs = []
    for record in records:
        source = record
        print(f'creating graph for {record["title"]}')
        if source["attributes"]["num_tokens"] >= MAX_TOKENS:
            root_node = get_page_graph_from_content(
                record,
                record["content"].replace(skip_string, ""),
                max_depth,
                coalesce=coalesce,
            )
            graphs.append((root_node, record))
    return graphs


def create_records_from_graphs(graphs):
    graph_records = []
    for graph, record in graphs:
        html = ""
        graph_records.extend(create_records_from_page_graph(graph, html))
    return graph_records


def create_graph_records(base_records, max_depth=2, coalesce=True):
    graphs = create_graphs_from_index(
        "", max_depth=max_depth, coalesce=coalesce, records=base_records
    )
    seen = {graph[0].url for graph in graphs}
    records = []
    for record in base_records:
        if record["url"] not in seen:
            records.append(convert_record(record))

    graph_records = create_records_from_graphs(graphs)
    records.extend(graph_records)
    return records


def remove_code_blocks(text):
    # print(text)
    return re.sub(r"```.*?```", "", text, flags=re.DOTALL)


def get_top_level_headings(content):
    md_content = content.strip()
    top_level_headings = []
    if md_content:
        headings: T.List = get_headings_from_md_safe(remove_code_blocks(md_content))
        top_level_headings: T.List = [
            {
                "anchor": None,
                "url": heading["url"],
                "html_tag": heading["html_tag"],
                "content": heading["content"].replace("///#", "#"),
                "content_html": "",
            }
            for heading in headings
        ]
    return top_level_headings


def replace_code_blocks(md):
    blocks = re.findall(r"```.*?```", md, re.DOTALL)
    for block in blocks:
        md = md.replace(block, "\n\n")
    return md


def get_headings_from_md(markdown_string) -> T.List:
    markdown_string = replace_code_blocks(markdown_string)
    pattern_url = r"^(#{1,6})\s((?:.*?\s)?)(?:\[(.*?)\]\((.*?)\))((?:\s.*)?)$"
    pattern_no_url = r"^(#{1,6})\s(.*)$"
    pattern_pre_url = r"^(#{1,6})\s.*(?:\[(?:.*?)\]\((https://.*?)\))(.*)?$"
    lines = markdown_string.split("\n")
    lines = [l.strip() for l in lines]
    level_text_url = []
    for line in lines:
        match_url = re.search(pattern_url, line)
        match_no_url = re.search(pattern_no_url, line)
        match_pre_url = re.search(pattern_pre_url, line)
        if match_url:
            level = len(match_url.group(1))
            text = (
                f"{match_url.group(2)}{match_url.group(3)}{match_url.group(5)}".strip()
            )
            url = match_url.group(4).split(" ")[0]
            level_text_url.append((level, text, url))
        elif match_pre_url:
            level = len(match_pre_url.group(1))
            text = f"{match_pre_url.group(3)}".strip()
            url = match_pre_url.group(2).split(" ")[0]
            level_text_url.append((level, text, url))
        elif match_no_url:
            level = len(match_no_url.group(1))
            text = match_no_url.group(2).strip()
            level_text_url.append((level, text, None))

    level_text_urls = level_text_url

    # print(level_text_urls)
    if not level_text_urls:
        return []

    level_text_url_by_level = {}
    for level, text, url in level_text_urls:
        if level not in level_text_url_by_level:
            level_text_url_by_level[level] = []
        level_text_url_by_level[level].append(
            {
                "level": level,
                "html_tag": f"h{level}",
                "content": invert_escape_md(text),
                "url": url,
            }
        )

    first_level_with_multiple_headings = None
    for i in range(1, 7):
        entry = level_text_url_by_level.get(i)
        if entry and len(entry) > 1:
            first_level_with_multiple_headings = i
            break

    if first_level_with_multiple_headings is not None:
        return [
            {
                "html_tag": entry["html_tag"],
                "content": entry["content"],
                "url": entry["url"],
            }
            for entry in level_text_url_by_level[first_level_with_multiple_headings]
        ]
    else:
        first_html_tag, first_content, first_url = level_text_urls[0]
        return [
            {
                "html_tag": first_html_tag,
                "content": first_content,
                "url": first_url,
            }
        ]


def get_headings_from_md_safe(markdown_string) -> T.List:
    try:
        return get_headings_from_md(markdown_string)
    except Exception:
        print("Error parsing markdown string", markdown_string)
        return []


RE_INV_SPACE = re.compile(r"\\\+")
RE_INV_ORDERED_LIST_MATCHER = re.compile(r"\\\.")
RE_INV_UNORDERED_LIST_MATCHER = re.compile(r"\\(-|\*|\+)")
RE_INV_MD_CHARS_MATCHER = re.compile(r"\\([\\\[\]\(\)])")
RE_INV_MD_CHARS_MATCHER_ALL = re.compile(r"\\([`\*_{}\[\]\(\)#!])")
RE_INV_MD_DOT_MATCHER = re.compile(r"(\\d+)\\.")
RE_INV_MD_PLUS_MATCHER = re.compile(r"\\\+")
RE_INV_MD_DASH_MATCHER = re.compile(r"\\-")
RE_INV_MD_BACKSLASH_MATCHER = re.compile(r"\\([%s])" % re.escape(r"\`*_{}[]()#+-.!"))


def invert_escape_md(text: str) -> str:
    text = RE_INV_MD_BACKSLASH_MATCHER.sub(r"\1", text)
    text = RE_INV_MD_CHARS_MATCHER.sub(r"\1", text)
    text = RE_INV_MD_DOT_MATCHER.sub(r"\1.", text)
    text = RE_INV_MD_PLUS_MATCHER.sub(r"+", text)
    text = RE_INV_MD_DASH_MATCHER.sub(r"-", text)

    return text
