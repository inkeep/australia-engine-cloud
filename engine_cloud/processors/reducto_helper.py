import requests
import json
import os
import tempfile
import backoff
from engine_cloud.config import settings
import html2text
from engine_cloud.utils.helpers import get_content_hash, get_num_tokens
from engine_cloud.utils.mongo import upload_batches


def parse_document_from_url(document_url: str):
    url = "https://platform.reducto.ai/parse"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "Authorization": f"Bearer {settings.REDUCTO_API_KEY}",
    }
    body = {
        "document_url": document_url,
        "advanced_options": {"table_output_format": "md"},
    }
    response = requests.post(url, headers=headers, json=body)
    print(response.status_code)
    print(response.text)
    return response.json()["result"]


@backoff.on_exception(backoff.constant, Exception, max_tries=5, interval=1)
def upload_documents(filename: str):
    upload_form = requests.post(
        "https://platform.reducto.ai/upload",
        headers={"Authorization": f"Bearer {settings.REDUCTO_API_KEY}"},
    ).json()
    print(upload_form)
    requests.put(
        upload_form["presigned_url"],
        data=open(filename, "rb"),
    )
    response = requests.post(
        "https://platform.reducto.ai/parse",
        json={"document_url": upload_form["file_id"]},
        headers={"Authorization": f"Bearer {settings.REDUCTO_API_KEY}"},
    )
    response.raise_for_status()
    return filename, response.json()["result"]


def upload_folder(folder_path: str):
    results = {}
    failed = []
    for file in os.listdir(folder_path):
        try:
            filename, reducto_response = upload_documents(
                os.path.join(folder_path, file)
            )
            results[filename] = reducto_response
        except Exception as e:
            print(f"Error uploading {file}: {e}")
            failed.append(file)
    return results, failed


def get_title_from_content(content: str):
    lines = content.split("\n")
    for line in lines:
        if line.startswith("# ") or "Scope: " in line:
            return line.replace("# ", "").replace("Scope: ", "")
    return None


def convert_and_upload_reducto_results(
    results: dict, url, index_name, source_id, org, project_id
):
    for filename, result in results.items():
        reducto_result = result
        title = filename.split(".")[0]
        print(title)
        if reducto_result["type"] == "full":
            records = merge_chunks(reducto_result, title, url)
            upload_batches(records, index_name, source_id, org, project_id)
        else:
            download_url = reducto_result["url"]
            tmpfile = tempfile.mktemp()
            response = requests.get(download_url)
            with open(tmpfile, "wb") as f:
                f.write(response.content)
            title = get_title_from_content(
                json.load(open(tmpfile))["chunks"][0]["content"]
            )
            records = merge_chunks(json.load(open(tmpfile)), title, url)
            upload_batches(records, index_name, source_id, org, project_id)


def split_content_into_sections(content: str):
    sections = {}
    lines = content.split("\n")
    current_title = None
    current_content = []

    for line in lines:
        if line.startswith("# "):
            if " (cont.)" not in line:
                # Extract base title without "(cont.)" or similar suffixes
                base_title = line.split(" (cont")[0].strip()

                # If we have a previous section, save it
                if current_title:
                    if current_title not in sections:
                        sections[current_title] = "\n".join(current_content)

                # Start new section
                current_title = base_title
                current_content = [line]
        else:
            current_content.append(line)

    # Don't forget to add the last section
    if current_title and current_title not in sections:
        sections[current_title] = "\n".join(current_content)


def get_sections_from_chunks(filename: str):
    with open(filename, "r") as file:
        data = json.load(file)
    chunks = data["chunks"]
    full_content = "\n".join([chunk["content"] for chunk in chunks])

    # Split content into sections by titles
    sections = {}
    lines = full_content.split("\n")
    current_title = None
    current_content = []

    for line in lines:
        if line.startswith("# "):
            if " (cont.)" not in line:
                # Extract base title without "(cont.)" or similar suffixes
                base_title = line.split(" (cont")[0].strip()

                # If we have a previous section, save it
                if current_title:
                    if current_title not in sections:
                        sections[current_title] = "\n".join(current_content)

                # Start new section
                current_title = base_title
                current_content = [line]
        else:
            current_content.append(line)

    # Don't forget to add the last section
    if current_title and current_title not in sections:
        sections[current_title] = "\n".join(current_content)

    return sections


def upload_reducto_records(
    records,
    index_name: str,
    source_id: str,
    org: str,
    project_id: str,
):
    upload_batches(
        records,
        index_name,
        source_id,
        org,
        project_id,
    )


def extract_and_convert_tables(content: str):
    text_maker = html2text.HTML2Text()

    # Split content by HTML table tags
    parts = content.split("<table>")

    # First part is regular markdown
    result = [parts[0]]

    # Process each table and following content
    for part in parts[1:]:
        if "</table>" in part:
            # Split into table and remaining content
            table_content, remaining = part.split("</table>", 1)

            # Reconstruct full table HTML
            table_html = f"<table>{table_content}</table>"

            # Convert table to markdown
            table_markdown = text_maker.handle(table_html)

            # Add converted table and remaining content
            result.append(table_markdown)
            result.append(remaining)

    # Join all parts back together
    return "".join(result)


def merge_chunks(
    data: dict,
    title: str,
    url: str,
):
    records = []
    current_chunk = ""
    current_page = 0
    current_tokens = 0
    MAX_TOKENS = 1000
    previous_header = None

    for i, chunk in enumerate(data["chunks"]):
        content = chunk["content"]
        page_num = min([block["bbox"]["page"] for block in chunk["blocks"]])
        print(f"Processing chunk {i}")
        if "<table" in content:
            content = extract_and_convert_tables(content)

        chunk_tokens = get_num_tokens(content)

        # Check if adding this chunk would exceed the token limit
        if current_tokens + chunk_tokens > MAX_TOKENS:
            # Save current chunk if it's not empty
            if current_chunk:
                header = extract_chapter_from_chunk(current_chunk)
                if not header:
                    header = previous_header
                else:
                    previous_header = header
                records.append(
                    {
                        "record_id": get_content_hash(current_chunk),
                        "format": "md",
                        "content": current_chunk,
                        "url": url + f"#page={current_page}",
                        "title": title,
                        "attributes": {
                            "num_tokens": get_num_tokens(current_chunk),
                            "page_num": current_page,
                            "breadcrumbs": json.dumps(
                                [
                                    title,
                                    header,
                                ],
                            ),
                        },
                        "extra_attributes": {},
                    }
                )
            # Start new chunk
            current_chunk = content
            current_tokens = chunk_tokens
            current_page = page_num
        else:
            # Add to current chunk
            current_chunk = current_chunk + "\n" + content if current_chunk else content
            current_tokens += chunk_tokens

    # Don't forget to add the last chunk
    if current_chunk:
        header = extract_chapter_from_chunk(current_chunk)
        if not header:
            header = previous_header
        else:
            previous_header = header
        records.append(
            {
                "record_id": get_content_hash(current_chunk),
                "format": "md",
                "content": current_chunk,
                "url": url + f"#page={current_page}",
                "title": title,
                "attributes": {
                    "num_tokens": get_num_tokens(current_chunk),
                    "page_num": current_page,
                    "breadcrumbs": json.dumps(
                        [
                            title,
                            header,
                        ]
                    ),
                },
                "extra_attributes": {},
            }
        )

    # Add content_full field combining previous, current and next record content
    for i in range(len(records)):
        prev_content = records[i - 1]["content"] if i > 0 else ""
        curr_content = records[i]["content"]
        next_content = records[i + 1]["content"] if i < len(records) - 1 else ""

        merged_content = "\n".join(
            filter(None, [prev_content, curr_content, next_content])
        )
        records[i]["extra_attributes"]["content_full"] = merged_content

    print(f"Created {len(records)} records")
    return records


def extract_chapter_from_chunk(chunk: str):
    for line in chunk.split("\n"):
        output = ""
        # if (
        #     line.startswith("## SUBTITULO")
        #     or line.startswith("## SUBCAPITULO")
        #     or line.startswith("## CAPITULO")
        #     or line.startswith("## Sección ")
        #     or line.startswith("## SUBCAPÍTULO")
        # ):
        #     output = line.split("## ")[1]
        # if line.startswith("# SUBCAPÍTULO") or line.startswith("# Sección"):
        #     output = line.split("# ")[1]
        if (line.startswith("# ") or line.startswith("## ")) and (
            "# Descripción del" not in line and "## Validación" not in line
        ):
            output = line.replace("## ", "").replace("# ", "")
        if output:
            output = output.replace("(cont.)", "").strip().split(". ")[0].split(" [")[0]
            return " ".join([x.capitalize() for x in output.split()])
    return None


def get_page_mapping_from_text(text: str, start_text: str):
    start = False
    previous_line_text = start_text
    page_mappings = {}
    for i, line in enumerate(text.split("\n")):
        if not line:
            continue
        line = line.replace("## ÍNDICE (cont.)", "")
        if (
            line
            == "ANEJO T CORPORACIÓN: ADICIÓN A LA CONTRIBUCIÓN POR FALTA DE PAGO DE LA CONTRIBUCIÓN ESTIMADA DE CORPORACIONES 205"
        ):
            page_mappings[205] = line.replace(" 205", "")
            continue
        if (
            line
            == "ANEJO G CORPORACIÓN: DETALLE DE PÉRDIDAS NETAS EN OPERACIONES INCURRIDAS EN AÑOS ANTERIORES 184"
        ):
            page_mappings[184] = line.replace(" 184", "")
            continue

        if start:
            if line.isdigit():
                if int(line) not in page_mappings:
                    page_mappings[int(line)] = previous_line_text

        if start_text in line:
            start = True

        previous_line_text = line

    return page_mappings


def extract_page_mappings(table_content: str) -> dict:
    # Remove HTML tags and split into lines
    lines = (
        table_content.replace("<table>", "")
        .replace("</table>", "")
        .replace("<tr>", "")
        .replace("</tr>", "\n")
        .replace("<td>", "")
        .replace("</td>", "|")
        .replace('<th colspan="3">', "")
        .replace("</th>", "|")
        .strip()
        .split("\n")
    )

    page_mappings = {}
    current_text = ""

    for line in lines:
        # Skip empty lines
        if not line.strip():
            continue

        # Split by the separator we added
        parts = [p.strip() for p in line.split("|") if p.strip()]

        # Skip header row
        if "PLANILLA DE CONTRIBUCIÓN" in line:
            continue

        # If we find a number at the end, it's a complete entry
        if parts and parts[-1].isdigit():
            # Combine all parts except the last one (page number)
            text = " ".join(parts[:-1]).strip()
            page_num = int(parts[-1])

            # If we have accumulated text from previous lines, combine it
            if current_text:
                text = current_text + " " + text
                current_text = ""

            if "213 ANEJO F- OTROS INGRESOS" in text:
                page_mappings[213] = "ANEJO F- OTROS INGRESOS"

            if "321 ANEJO L- INGRESO DE AGRICULTURA" in text:
                print("Found 321")
                page_mappings[321] = "ANEJO L- INGRESO DE AGRICULTURA"

            page_mappings[page_num] = (
                (
                    text.replace('<td colspan="3">', "")
                    .replace('<td colspan="4">', "")
                    .replace("</td>", "")
                )
                .replace("<th>", "")
                .replace("321 ANEJO L- INGRESO DE AGRICULTURA", "")
                .replace("213 ANEJO F- OTROS INGRESOS", "")
            )

        else:
            # If no page number, accumulate text for next line
            current_text = " ".join(parts).strip()

    return page_mappings


def find_closest_page(page_mappings: dict, target_page: int) -> int:
    # Get all page numbers from the dictionary
    pages = sorted(page_mappings.keys())

    # Find the closest page number that's <= target_page
    closest = pages[0]  # Initialize with first page
    for page in pages:
        if page <= target_page:
            closest = page
        else:
            break

    return closest


def update_breadcrumbs(mapping, records):
    for record in records:
        page_num = record["attributes"]["page_num"]
        closest = find_closest_page(mapping, page_num)
        record["attributes"]["breadcrumbs"] = json.dumps(
            [record["title"], mapping[closest].strip()]
        )
