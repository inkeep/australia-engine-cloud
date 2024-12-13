import anthropic
import time
from config import settings


def make_ticket_selection_prompt(ticket_content, org, description):
    prompt = f"""
    You are a helpful assistant from {org} ({description}). Forget everything you think you know about {org}.

You are given the text from a Zendesk Ticket from {org}.
<Instructions>
The Zendesk Ticket contains messages between a user and a support agent. 
</Instructions>

<TICKET>
{ticket_content}
</TICKET>

<Instructions>
Here is your task:
- Determine if the Zendesk Ticket contains any technical support questions or feature questions about {org}'s that are answered in the ticket messages.

Here are guidelines you must follow to complete this task:
- ONLY USE INFORMATION EXCLUSIVELY FROM THE ZENDESK TICKET. DO NOT USE ANY OUTSIDE INFORMATION.
- ANSWERS MUST BE GENERALLY USEFUL TO OTHER USERS NOT SPECIFIC TO THE PERSON ASKING THE QUESTION.
- Questions should relate to {org}'s technical product 
- There may be none, one, or many questions found in the ticket.
- The questions must have been answered by the comments in the ticket and be relevant to all users of {org}'s product.

Examples of answers that are not relevant to all users of {org}'s product:
- The answer is about a specific user's account or data.
- An answer is not relevant if it requires reaching out to a support agent.
- An answer is not relevant if it that requires a support agent to reach out to a user.

For each question you find, please ensure the following:
- This question HAS AN ANWSWER in the messages. 
- The answer is relevant to all users of {org}'s product.
- This question is not about the Zendesk Ticket itself.


If these criteria apply, then include the question in the list of questions you return. Otherwise, DO NOT include the question in the list of questions you return.
If there are no questions that meet these criteria, then return an empty list.

Follow these instructions to format your answer:
- Output your answer as a valid JSON object with the key "questions" that have answers from the Ticket.
- The value of "questions" is a list of questions that you found with answers from the comments.
- If there are no questions with answers the value of questions is an empty list.
- Questions without answers cannot be included in the output.
- The output should be inside a JSON dict in <json></json> tags, like so: 
<json>
{{"questions": ["question string", "question string", "question string"]}}
</json>

please site the text from the <TICKET> section that answers these questions DO NOT MAKE UP TEXT.

If you did not find any valid answered questions, or if you cannot find any text to site here or  please return an empty list, like so:
<json>
{{"questions": []}}
</json>

</Instructions>

{anthropic.AI_PROMPT}: Okay here are the list of questions that have answers and solved by the comments in the GitHub Discussions Post
    """.strip()
    return prompt.strip()


async def anthropic_get_chat_completion(output_completion=False, **kwargs):
    async with anthropic.AsyncAnthropic(
        api_key=settings.ANTHROPIC_API_KEY
    ) as anthropic_client:
        time_start = time.time()
        print("single anthropic_chat_completions")
        response = await anthropic_client.messages.create(**kwargs)
        time_end = time.time()
        print(
            "anthropic_chat_completions",
            time_end - time_start,
            # response["usage"]["total_tokens"],  # type: ignore
        )
        if output_completion:
            output_completion = response.content[0].text
            return output_completion.strip()

        return response


async def get_valid_questions_from_ticket(ticket_content, org, description):
    anthropic_client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)
    prompt = make_ticket_selection_prompt(ticket_content, org, description)
    prompt_tokens = anthropic_client.count_tokens(prompt)
    if prompt_tokens > 64000:
        if prompt_tokens > 64000:
            raise Exception(f"Document is too large: {ticket_content}")

    # params = {
    #     "model": llm_config.ANTHROPIC_CLAUDE_INSTANT_1_2,
    #     "prompt": prompt,
    #     "max_tokens_to_sample": 1000,
    #     "output_completion": True,
    #     "timeout": 60,
    # }
    # response = await anthropic_chat_completions_with_backoff_single(**params)

    # }
    params = {
        "model": "claude-3-haiku-20240307",
        "messages": [
            {"role": "user", "content": prompt},
        ],
        "max_tokens": 1000,
        "output_completion": True,
        "timeout": 60,
    }
    response = await anthropic_get_chat_completion(**params)
    return response
