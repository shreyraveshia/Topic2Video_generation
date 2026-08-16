
"""
pipeline_stages.py
===================

SESSION 2 (dummy version) -- placeholder implementations of our 4 stages.
Real logic gets swapped in over the next sessions.

Each function below stands in for one real pipeline stage:

    gather_info        -> Task 1: research topic OR scrape URL / transcript
    generate_storyboard -> Task 2: LLM turns raw content into scenes
    generate_assets     -> Task 3: images + narration audio per scene
    assemble_video       -> Task 4: FFmpeg stitches everything into the final mp4

Right now they just sleep briefly and return fake data, so we can verify
the orchestrator (caching, retries, state-passing) works correctly in
isolation. In later sessions we replace the BODY of each function with
real logic -- the function signature (dict in, dict out) and the fact that
each is registered via `@task(...)` does not need to change. This is the
"swap the implementation without touching the pipeline" property the
assignment asks for.
"""

from pathlib import Path
import random   # Used only to simulate occasional failures.
import time     # Used to simulate slow operations.

from orchestrator import task  # Imports our decorator from `orchestrator.py`.
# This lets us write:- @task(...)


CACHE_ROOT = Path(__file__).resolve().parent.parent / "cache"
# __file__ -> Python's special built-in variable containing the current file's path.
# This only works because both files sit in the same src/ folder — Python looks in the current folder first when resolving imports.


import os
from dotenv import load_dotenv ; import requests

from groq import Groq

load_dotenv()  # reads .env and loads GROQ_API_KEY into the environment

_groq_client = Groq(api_key=os.environ["GROQ_API_KEY"])
# Creates one reusable connection object to Groq's API, authenticated with your key. 
# We build this once, outside any function, 
# so we're not recreating a fresh connection on every single call — small efficiency detail. 
# The leading underscore signals "internal to this module."


def _research_topic(topic: str) -> str:
    """
    Ask Groq's LLM to act as a researcher for a topic-only input
    (no source material was provided by the user).
    """
    response = _groq_client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {
                "role": "system",  
                # "system" — instructions about how the AI should behave for this whole task 
                # (not something the "user" said — background rules).
                "content": (
                    "You are a careful researcher preparing background material "
                    "for a short 30-40 second video. Provide accurate, well-known, "
                    "widely-agreed-upon facts and explanations. Do not invent "
                    "statistics or studies. Write 3-5 short paragraphs covering: "
                    "what the topic means, why it happens/matters, and any "
                    "practical takeaway. Keep it factual and clear."
                ),
            },
            {"role": "user", "content": f"Topic: {topic}"},
        ],
        temperature=0.4, # 0.4 is a reasonable middle ground for "factual but not robotic."
    )
    return response.choices[0].message.content 
# choices[0] is the (first, and here only) generated answer; 
# .message.content is the actual text string we want.











import re
import trafilatura
from youtube_transcript_api import YouTubeTranscriptApi 
# Its purpose is-> Given a YouTube video ID, retrieve the video's transcript/captions when available.

# YouTube URL ->https://www.youtube.com/watch?v=dQw4w9WgXcQ -> 
# video ID -> dQw4w9WgXcQ->YouTubeTranscriptApi ->transcript


# `trafilatura` is an external Python package used to download web pages and extract the main textual content.
# `trafilatura.extract()` attempts to identify and extract that main content (actual article).

'''
`re` is Python's built-in **regular expression** module.

Regular expressions allow us to search for patterns in text.

We're using it mainly for:

* identifying YouTube URL formats
* extracting the YouTube video ID
* finding a `videoId` inside YouTube's search-result HTML

For example, this:
r"youtu\.be/([\w-]+)"

means approximately:
> Find `youtu.be/`, followed by characters that can form a YouTube video ID.
'''

# The `_` at the beginning-> is a Python naming convention indicating:
#> This is an internal/helper function.
# It isn't enforced by Python. It's simply a convention.

def _extract_youtube_video_id(url: str) -> str | None:   
    """
    Pulls the video ID out of common YouTube URL formats, e.g.:
      https://www.youtube.com/watch?v=abcXYZ123
      https://youtu.be/abcXYZ123
    Returns None if this doesn't look like a YouTube URL at all.
    """

# uses regex (regular expressions — pattern-matching for text). 
# Each pattern describes a shape to search for inside the URL string. 

# (?:...) is a non-capturing group (just for matching, not extracting),
#  while (...) around [\w-]+ is what gets captured — that's the actual video ID we want back. 
# We try 3 common URL shapes since people share YouTube links differently.

# We're creating a Python list. The list contains the different URL patterns we're willing to recognize.
    patterns = [

        r"(?:youtube\.com/watch\?v=)([\w-]+)", 
        # means "find youtube.com/watch?v= followed by a chunk of letters/numbers/dashes, and capture that chunk."

        r"(?:youtu\.be/)([\w-]+)", # Matches : https://youtu.be/abcXYZ123 (captured portion is abcXYZ123)
        r"(?:youtube\.com/embed/)([\w-]+)",
    ]
    #The parentheses:(...) ->create a capturing group.
    # `[\w-]` means:
    #  Match a word character or `-`.
    
# `\w` generally includes:
# A-Z
# a-z
# 0-9   ............. '-' -> allows hyphenated video IDs, which YouTube uses.

# The `+` means: -> One or more.    
# Captures something like-> abcXYZ123--> this is our video ID.  The `+` means we want the whole ID, not just the first character.


    for pattern in patterns: # Try each URL pattern one by one.
        match = re.search(pattern, url) # re.search()-> searches the entire `url` string for the given pattern.
        if match:
            return match.group(1)
        # Remember:- ([\w-]+) > is a capturing group, so `group(1)`: means-> Give me the text captured by the first parentheses.
    return None
# > Input `url` should be a string. 
# > The function returns either a `str` or `None`. ===> might find a video ID, or I might not.




def _search_youtube_video_id(query: str) -> str | None:

    """
We're no longer given a YouTube URL. -> We're given a **search query**. EX->query = "How to speak so that people want to listen TED"

The goal is:

search query
     ↓
YouTube search page
     ↓
find first video ID.  ---------This is primarily used for the TED case.---------

    Free, no-API-key YouTube search: hits the public search results page
    and pulls the first videoId it finds in the page's embedded data.
    """
    search_url = "https://www.youtube.com/results" # We're going to request->https://www.youtube.com/results with a query parameter.
    resp = requests.get(
        search_url,
        params={"search_query": query},
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=15, # If the request doesn't complete within 15 seconds, the request raises an exception.
    )

# If query = "Python tutorial" -> the resulting URL is conceptually:
# https://www.youtube.com/results?search_query=Python%20tutorial
#The library handles URL encoding.

# The `User-Agent` header tells the server what kind of client is making the request. 
# Without one, some websites may treat the request as suspicious or unusual. 
# Here we're saying approximately: > "This request looks like it came from a normal browser."

# Check HTTP status:-
    resp.raise_for_status() # This checks whether the HTTP request succeeded.
# For an unsuccessful HTTP status, `raise_for_status()` raises an exception.
# That exception then propagates to our task orchestrator.
# And that's where our retry mechanism becomes useful.

# YouTube request-> HTTP failure -> raise_for_status() -> exception-> CachedRetryableTask ->retry 

# Find Video ID 
    match = re.search(r'"videoId":"([\w-]{11})"', resp.text) # Now we're searching the HTML returned by YouTube.
    return match.group(1) if match else None

# regex looks for something like:-"videoId":"abcXYZ12345"
# > Exactly 11 characters. -> YouTube video IDs are typically 11 characters. 
#  The parentheses capture those 11 characters.

# if matched? found → video ID
# not found → None


def _get_page_title(url: str) -> str | None:
    downloaded = trafilatura.fetch_url(url) # This asks Trafilatura to download the webpage. If successful: HTML Cotent. If not successful: None.
    if downloaded is None:                  #  We can't get the title, so stop this branch.
        return None
    metadata = trafilatura.extract_metadata(downloaded) # Trafilatura examines the downloaded page and attempts to extract metadata.
    return metadata.title if metadata else None     # Potential metadata includes things like:- title, author, date, description

#  Return title
# metadata exists → title
# metadata doesn't exist → None

# trafilatura.extract_metadata-> this pulls out just page metadata like the title, which is present in plain HTML
'''
This function's purpose is -> Given a webpage URL, get its title.

Why do we need that?->Because for TED we're going to do:

TED URL
 ↓
TED page title
 ↓
YouTube search
 ↓
YouTube video
 ↓
transcript
'''



def _fetch_ted_transcript(url: str) -> str | None:
    """
    The goal: -> Try to obtain the transcript for a TED talk.

    TED's transcript is loaded via JavaScript, so we can't scrape it
    directly. Instead: get the talk's title from the page, search YouTube
    for "<title> TED", and fetch that video's transcript.
    
  TED transcript
       ↓
  not directly scraped
       ↓
  get TED title
       ↓
  search YouTube
       ↓
  get transcript
```
    """


    if "ted.com/talks" not in url:
        return None

    title = _get_page_title(url) # Now call our previous helper.
    if not title:
        return None  # Ex- Example: TED URL ->_get_page_title() -> "How to speak so that people want to listen"
# If we couldn't obtain the title, we can't search YouTube reliably. So stop.

    video_id = _search_youtube_video_id(f"{title} TED")
    if not video_id:
        return None
    # Then `_search_youtube_video_id()` searches YouTube for it.

    return _fetch_youtube_transcript(video_id)
'''
Now we have:

```text
TED title
 ↓
YouTube video ID
 ↓
YouTube transcript
'''









def _fetch_youtube_transcript(video_id: str) -> str:
    api = YouTubeTranscriptApi()
    fetched = api.fetch(video_id)  # This asks the library -> Give me the transcript associated with this YouTube video. ->The result contains transcript snippets.
    full_text = " ".join(snippet.text for snippet in fetched)
    return full_text
'''
This function accepts -> YouTube video ID <-----> and returns -> complete transcript as a string


fetched = api.fetch(video_id) -->This asks the library -> Give me the transcript associated with this YouTube video.

The result contains transcript snippets.

Conceptually:
[
    snippet 1,
    snippet 2,
    snippet 3,
    ...
]
Each snippet has text.

# Combine snippets

full_text = " ".join(snippet.text for snippet in fetched)

This is a **generator expression** inside `" ".join()`.

Suppose:

snippet 1 → "Hello everyone"
snippet 2 → "today we are going to..."
snippet 3 → "talk about procrastination"

This---snippet.text for snippet in fetched

produces:

"Hello everyone"
"today we are going to..."
"talk about procrastination"

Then:

" ".join(...)

joins them with spaces-> Hello everyone today we are going to... talk about procrastination

So the final output is one large string-- as full text.


Now:

YouTube transcript
        ↓
single string

'''




def _scrape_article(url: str) -> str:  # This handles normal article URLs.
    downloaded = trafilatura.fetch_url(url)
    if downloaded is None:
        raise RuntimeError(f"Could not download page: {url}")
    text = trafilatura.extract(downloaded)              # output: main articel text
    if not text:
        raise RuntimeError(f"Could not extract article text from: {url}") # Again, failure gets propagated to the orchestrator for retry.
    return text

'''
if downloaded is None:
    raise RuntimeError(f"Could not download page: {url}")

This time we don't return `None`.---->>>We raise an exception.

Why? -------->Because this isn't an optional failure.

If we're supposed to gather information from this URL and couldn't download it, the task genuinely failed.
And because `gather_info` is wrapped by our orchestrator:

RuntimeError
   ↓
CachedRetryableTask
   ↓
retry

So this is a very intentional connection between `pipeline_stages.py` and `orchestrator.py`.
'''


def _gather_from_url(url: str) -> dict:
    """
    Branches: TED URL -> find title -> search YouTube -> transcript.
              Plain YouTube URL -> transcript API directly.
              Anything else -> treat as a normal article page.
    """
    ted_transcript = _fetch_ted_transcript(url)
    if ted_transcript:
        return {"source_type": "url", "url_kind": "ted_transcript_via_search", "raw_content": ted_transcript}

    video_id = _extract_youtube_video_id(url)
    if video_id:
        raw_content = _fetch_youtube_transcript(video_id)
        return {"source_type": "url", "url_kind": "video_transcript", "raw_content": raw_content}

    raw_content = _scrape_article(url)
    return {"source_type": "url", "url_kind": "article", "raw_content": raw_content}

'''
Its job is -> Decide which URL-handling strategy should be used.

The 3 possibilities are:

TED
 ↓
YouTube transcript

YouTube
 ↓
YouTube transcript

Other URL
 ↓
Article scraping

URL_kind -> tells us how the content was obtained

every URL branch produces the same broad structure:
{
    "source_type": "url",
    "url_kind": "...",
    "raw_content": "..."
}
'''














import json as json_lib


def _build_storyboard_prompt(raw_content: str, source_type: str, url_kind: str | None) -> str:
    grounding_rule = ""
    if source_type == "url":
        grounding_rule = (
    "IMPORTANT: This content comes from a real source. Every narration line "
    "must be directly supported by the source text below. Do not invent facts, "
    "statistics, or claims that are not present in the source -- this includes "
    "adding conclusions, themes, or phrases that sound plausible but were not "
    "actually stated (e.g. do not add words like 'connection' or 'trust' as a "
    "takeaway unless the source itself uses that word or a very close paraphrase). "
    "Before finalizing, mentally check each narration line against the source "
    "and remove anything you cannot point to directly in the text.\n\n"
)
        # only URL-sourced content gets the strict "don't invent beyond the source"

    return f"""{grounding_rule}Source content:
\"\"\"
{raw_content}
\"\"\"

Task: Turn this into a storyboard for a 30-40 second vertical video (9:16, TikTok/Reels-style).

Step 1 - Selection: This source likely contains more material than 35 seconds can hold.
Identify the 4 to 6 most important, most story-worthy points. Prioritize points that
together form a coherent narrative arc (hook -> context/explanation -> resolution/takeaway),
over points that are individually interesting but disconnected.

Step 2 - Storyboard: Turn the selected points into 6 to 7 scenes. HARD CONSTRAINT: every
single scene's duration_s MUST be either exactly 5 or exactly 6 -- no other value is
allowed (not 7, not 8, not 4). This matches what AI video generation models can natively
produce in one call. Each scene needs:
- narration: one short spoken sentence that fills most of the scene's duration at a natural
  speaking pace (~2.5 words/sec) -- roughly 12-15 words for a 5-6 second scene. Conversational
  tone, not written-essay style.
- visual: a concrete, filmable description of what should be shown on screen (subject,
  action, setting -- specific enough to generate an image/short clip from)
- duration_s: this scene's length in seconds (all scenes summed should total 30-40s)
Example of correctly-paced narration for a 5-second scene (12-13 words):
"Most of us know that feeling — staring at a task, unable to start."
(NOT "We procrastinate" -- too short, leaves dead air when spoken aloud)

Step 3 - Style Guide: Write ONE short visual style description (palette, lighting, subject
consistency, camera feel) that will be applied to every scene's image, so the whole video
looks visually consistent.

Respond with ONLY valid JSON, no other text, in exactly this shape:
{{
  "style_guide": "string describing the consistent visual style",
  "scenes": [
    {{"scene_id": 1, "narration": "...", "visual": "...", "duration_s": 8}},
    {{"scene_id": 2, "narration": "...", "visual": "...", "duration_s": 8}}
  ]
}}"""


def _generate_storyboard_llm(raw_content: str, source_type: str, url_kind: str | None = None) -> dict:
    prompt = _build_storyboard_prompt(raw_content, source_type, url_kind)

    response = _groq_client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a short-form video director. You respond with ONLY valid "
                    "JSON matching the exact schema requested. No markdown code fences, "
                    "no explanation text before or after -- just the raw JSON object."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        temperature=0.6,
    )

    raw_text = response.choices[0].message.content.strip()

    # Defensive: strip markdown code fences if the model added them anyway.
    if raw_text.startswith("```"):
        raw_text = raw_text.strip("`")
        if raw_text.startswith("json"):
            raw_text = raw_text[4:]
        raw_text = raw_text.strip()

    return json_lib.loads(raw_text)



















# Real image generation, using the Style Guide

import urllib.parse
# need it because Pollinations' endpoint puts your prompt inside the URL.


ASSETS_DIR = CACHE_ROOT.parent / "outputs" / "scene_assets" # This defines where your generated images and audio will be stored.


def _generate_scene_image(visual_description: str, style_guide: str, scene_id: int, run_id: str) -> str:
    """
    Calls Pollinations.ai's free, no-key image generation endpoint.
    Returns the local file path where the image was saved.
    """

    full_prompt = f"{visual_description}, {style_guide}" 
    # This is the actual mechanism behind "visual continuity" we designed
    # — every single scene's image request gets the same style guide text appended, 
    # so Pollinations generates images that share a consistent look, even though each scene's subject/action is different.
    
    encoded_prompt = urllib.parse.quote(full_prompt)
    # Pollinations.ai's API works by putting your actual text prompt directly into the URL itself 
    # (e.g., image.pollinations.ai/prompt/a person at a desk). But URLs can't contain spaces, commas, or 
    # special characters safely 

    # — urllib.parse.quote() -> converts those into URL-safe escape codes (e.g., a space becomes %20). 
    # Without this, our request would likely fail or produce a broken URL.


    url = f"https://image.pollinations.ai/prompt/{encoded_prompt}"
    params = {"width": 1024, "height": 1820, "nologo": "true"}  # portrait-leaning canvas

    # We're requesting a portrait-oriented image close to our eventual video's proportions  (we'll do final exact 
    # 1080×1920 cropping/padding in Stage 4's FFmpeg step — this just gets us close from the start, which reduces distortion). 
    # nologo=true asks Pollinations to skip adding their watermark.

    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status() 

    # This checks whether the HTTP request succeeded.
    # If the HTTP request failed (bad response code, like a 500 server error), this deliberately throws an exception 
    # — which, as always, our orchestrator will catch and retry automatically.

    # That's good because your pipeline can then recognize:
    # "This scene image wasn't successfully generated."
    # rather than blindly saving bad data.

    scene_dir = ASSETS_DIR / run_id


    #outputs/scene_assets/run_001
# We're organizing saved images by run_id (we'll define this shortly) so that different pipeline runs 
# (e.g., your topic video vs. your TED video) don't overwrite each other's images in the same folder.

    scene_dir.mkdir(parents=True, exist_ok=True)
    image_path = scene_dir / f"scene_{scene_id}.png"

    # Suppose : scene_id = 3 ; image_paths becomes-> outputs/scene_assets/run_001/scene_3.png

    image_path.write_bytes(response.content)
    # response.content-> is the actual raw image data (bytes) Pollinations sent back. 
    # .write_bytes() saves it directly to disk as a real .png file.

    # So::
# Pollinations
#   ↓
#HTTP response
#   ↓
#response.content ====> contains "raw image data (bytes)"
#   ↓
#write_bytes()
#   ↓
# scene_3.png

    return str(image_path)

# Why str()?  --> Because image_path is a Path object.
#The function promises: -> str

#So it converts: Path("outputs/scene_assets/run_001/scene_3.png") --into-- "outputs/scene_assets/run_001/scene_3.png"
#The caller can then easily store/use that path.








# Real narration audio generation

import edge_tts # The text-to-speech library.
import asyncio # The text-to-speech library is asynchronous, so we need Python's asyncio to run it.
# Python's asynchronous programming framework.

async def _generate_scene_audio_async(narration_text: str, output_path: Path) -> None:
    communicator = edge_tts.Communicate(narration_text, voice="en-US-AriaNeural")
    await communicator.save(str(output_path))

# narration_text->This comes directly from your storyboard.
# output_path ->Where should the generated MP3 be saved?
#For example -> outputs/scene_assets/run_001/scene_1.mp3

# save() needs to communicate with the TTS service and wait for the audio to be generated.
# await essentially means:
# "Wait for this asynchronous operation to finish before continuing this async function."

def _generate_scene_audio(narration_text: str, scene_id: int, run_id: str) -> str:
    scene_dir = ASSETS_DIR / run_id
    scene_dir.mkdir(parents=True, exist_ok=True)
    audio_path = scene_dir / f"scene_{scene_id}.mp3"

    asyncio.run(_generate_scene_audio_async(narration_text, audio_path))
    # asyncio.run() -> creates/runs the necessary event loop and waits until the async function completes.

    # ...this line, inside a normal, regular (non-async) function, is how you "bridge" into async code from ordinary code 
    # — asyncio.run()-> starts up what's needed to run an async function and waits for it to finish before continuing, so from the outside, 
    # _generate_scene_audio behaves like any normal function we can call from generate_assets just like everything else.

    return str(audio_path)

# call function
  #   ↓
# wait
   #  ↓
# audio generated
  #   ↓
# continue

# (async/await — a way of writing code that can pause and resume around slow I/O operations, 
# like network calls, without blocking everything else).







from mutagen.mp3 import MP3


def _get_audio_duration(audio_path: str) -> float:
    """
    Returns the real, measured duration (in seconds) of a generated
    narration audio file -- this is our ground truth for scene timing,
    since LLM-predicted durations proved unreliable (see Session 3 notes).
    """
    audio = MP3(audio_path)
    return audio.info.length















# the Ken Burns fallback

import numpy as np
from PIL import Image
from moviepy import VideoClip


def _apply_ken_burns(image_path: str, duration_s: float, output_path: str) -> str:
    """
    Fallback motion: slow zoom across a still image, keeping frame
    dimensions constant throughout (required for correct video encoding --
    a naive resize-per-frame approach corrupts the video because the
    codec requires every frame to be the exact same size).
    """
    img = Image.open(image_path).convert("RGB")
    w, h = img.size
    w -= w % 2  # libx264 requires even width/height
    h -= h % 2
    img = img.crop((0, 0, w, h))
    img_arr = np.array(img)

    def make_frame(t):
        scale = 1 + 0.04 * (t / duration_s)  # 1.0x -> 1.04x zoom
        new_w, new_h = int(w * scale), int(h * scale)
        resized = np.array(Image.fromarray(img_arr).resize((new_w, new_h)))
        x0 = (new_w - w) // 2
        y0 = (new_h - h) // 2
        return resized[y0:y0 + h, x0:x0 + w]  # crop back to constant (h, w)

    clip = VideoClip(make_frame, duration=duration_s)
    clip.write_videofile(output_path, fps=24, codec="libx264", audio=False, logger=None)
    return output_path

'''
# w -= w % 2 and h -= h % 2 — forces both dimensions to be even, fixing the odd-height bug

# Instead of ImageClip(...).resized(...), 
 we now build the video frame-by-frame ourselves with VideoClip(make_frame, duration=duration_s) 
 — make_frame(t) is a function we write that, given a timestamp, returns the exact pixel array for that frame


 # Inside make_frame: 
  we resize a numpy copy of the original image larger (simulating "zooming in"), 
  then crop it back down to the original constant (w, h) size, centered 
  — this is the "look through a fixed window while the underlying image grows behind it" trick that keeps 
  output dimensions constant while still producing real, visible zoom motion
'''





# ImageClip(image_path) —> moviepy loads a still image as a "clip" (a video made from one frame, repeated)

# .with_duration(duration_s) —> sets how long this clip should play for (our real, measured audio duration from earlier — we'll wire that in shortly

# .resized(lambda t: 1 + 0.04 * (t / duration_s)) — this is the actual Ken Burns motion. 
# lambda t: ... defines a function of time t (moviepy calls this once per frame, feeding in the current timestamp). 
# At t=0 (start), the scale factor is 1 + 0.04*(0) = 1.0 (normal size). At t=duration_s (end), it's 1 + 0.04*(1) = 1.04 (4% zoomed in). 
# So across the whole clip, the image smoothly scales from 100% to 104% — a subtle, slow zoom, not jarring.

# .write_videofile(..., fps=24, codec="libx264", audio=False) — renders the actual .mp4 file. 
# fps=24 matches our PDF requirement. libx264 is a standard, widely-compatible video codec. 
# audio=False because this clip has no sound of its own — narration audio gets layered on separately in the final assembly stage.









# the primary path — real AI video via Hugging Face

from huggingface_hub import InferenceClient

_hf_client = InferenceClient(provider="fal-ai", api_key=os.environ.get("HF_TOKEN"))


def _generate_ai_video_clip(image_path: str, visual_description: str, output_path: str) -> str:
    """
    Primary motion path: animates the scene's still image into a real
    short AI-generated video clip using LTX-Video.
    """
    with open(image_path, "rb") as f:
        image_bytes = f.read()

    video_bytes = _hf_client.image_to_video(
        image_bytes,
        prompt=visual_description,
        model="Lightricks/LTX-Video",
    )

    with open(output_path, "wb") as f:
        f.write(video_bytes)

    return output_path



from orchestrator import task, logger
# Combine both into one function with automatic fallback

def _generate_scene_motion(
    image_path: str, visual_description: str, duration_s: float,
    scene_id: int, run_id: str,
) -> dict:
    """
    Tries real AI video generation first. If it fails for any reason,
    automatically falls back to Ken Burns on the still image, so one
    flaky generation never fails the whole pipeline.
    """
    scene_dir = ASSETS_DIR / run_id
    scene_dir.mkdir(parents=True, exist_ok=True)
    video_path = str(scene_dir / f"scene_{scene_id}_motion.mp4")

    try:
        _generate_ai_video_clip(image_path, visual_description, video_path)
        return {"motion_path": video_path, "motion_type": "ai_generated"}
    except Exception as exc:
        logger.warning(
            "Scene %d: AI video generation failed (%s), falling back to Ken Burns",
            scene_id, exc,
        )
        _apply_ken_burns(image_path, duration_s, video_path)
        return {"motion_path": video_path, "motion_type": "ken_burns_fallback"}









































# after this runs, gather_info becomes a full CachedRetryableTask object (not a plain function anymore), 
# configured to cache its results into cache/gather_info/.

# > Register the following function as a task called `gather_info`, 
# and cache its results under `cache/gather_info`.

@task("gather_info", cache_dir=CACHE_ROOT / "gather_info") # This is our decorator.
def gather_info(inputs: dict) -> dict:           # The actual task function.
                                                 # it accepts input & returns dictionary.
    user_input = inputs["initial_input"]["query"].strip()   
    is_url = user_input.startswith("http://") or user_input.startswith("https://")

    if not is_url:
        raw_content = _research_topic(user_input)
        return {
            "source_type": "topic",
            "raw_content": raw_content,
        }

    return _gather_from_url(user_input)


@task("storyboard", cache_dir=CACHE_ROOT / "storyboard")
def generate_storyboard(inputs: dict) -> dict:
    gather_info_output = inputs["gather_info"]
    raw_content = gather_info_output["raw_content"]
    source_type = gather_info_output["source_type"]
    url_kind = gather_info_output.get("url_kind")

    storyboard = _generate_storyboard_llm(raw_content, source_type, url_kind)

    # Duration is a constraint WE defined (matching AI video model output length),
    # not a fact the LLM needs to "get right" -- so instead of retrying and hoping,
    # we deterministically clamp it ourselves. Guarantees compliance, costs zero
    # extra API calls, and is more reliable than repeated retries for a rule this
    # simple.
    for scene in storyboard["scenes"]:
        scene["duration_s"] = min(6, max(5, scene["duration_s"]))

    total_duration = sum(scene["duration_s"] for scene in storyboard["scenes"])
    if not (25 <= total_duration <= 45):
        raise ValueError(
            f"Storyboard duration {total_duration}s is outside acceptable range "
            f"(target 30-40s, allowing some tolerance) even after clamping scene "
            f"lengths -- likely too few/many scenes returned. LLM output may need retry."
        )

    # Loose floor only: catches genuinely degenerate narration (empty/near-empty).
    # Real pacing/timing is authoritatively determined in Stage 3 by measuring
    # actual generated TTS audio length, since LLM-predicted word counts proved
    # unreliable across repeated testing.
    MIN_WORDS = 5
    for scene in storyboard["scenes"]:
        word_count = len(scene["narration"].split())
        if word_count < MIN_WORDS:
            raise ValueError(
                f"Scene {scene['scene_id']} narration is too sparse: {word_count} words "
                f"(minimum {MIN_WORDS}) -- likely malformed LLM output. May need retry."
            )

    return storyboard


@task("assets", cache_dir=CACHE_ROOT / "assets")
def generate_assets(inputs: dict) -> dict:
    scenes = inputs["storyboard"]["scenes"]
    style_guide = inputs["storyboard"]["style_guide"]
    run_id = inputs["initial_input"].get("run_id", "default")

   # Why this approach: 
   # we're generating a short, stable ID from the input itself (same technique as our cache-hashing from Session 1) 
   # — so the same input always produces the same run_id, and different inputs get different folders automatically, 
   # with zero extra bookkeeping needed.

    scene_assets = []
    for scene in scenes:
        image_path = _generate_scene_image(
            scene["visual"], style_guide, scene["scene_id"], run_id
        )
        audio_path = _generate_scene_audio(
            scene["narration"], scene["scene_id"], run_id
        )
        real_duration = _get_audio_duration(audio_path)


        # For every scene in the storyboard, we generate its real image, real audio, and measure the real duration 
        # — three genuine artifacts per scene, not fakes.


        scene_assets.append({
            "scene_id": scene["scene_id"],
            "narration": scene["narration"],
            "image_path": image_path,
            "audio_path": audio_path,
            "planned_duration_s": scene["duration_s"],
            "actual_duration_s": round(real_duration, 2),
        })

    return {"scene_assets": scene_assets}


@task("assembly", cache_dir=CACHE_ROOT / "final")
def assemble_video(inputs: dict) -> dict:
    scene_assets = inputs["assets"]["scene_assets"]
    time.sleep(1)
    return {"final_video_path": "[DUMMY]/final_video.mp4", "num_scenes": len(scene_assets)}