# Topic-to-Video Generation

A Python pipeline that takes a plain-text topic or a source URL and produces
a single 30–40 second, 1080×1920 (9:16) portrait video with narration and
captions — built as an orchestrated, cached, retryable task graph.

See **`APPROACH.md`** for the full design write-up (architecture, tradeoffs,
and a list of real bugs found and fixed during development).
See **`notebooks/topic_to_video.ipynb`** for the same pipeline in notebook form.

---

## Requirements

- Python 3.10+
- A free [Groq](https://console.groq.com) API key (no credit card required)
- No other paid services or API keys are used anywhere in this project.

---

## Setup

```bash
# 1. Clone/unzip the project, then from the project root:
python -m venv venv

# 2. Activate the virtual environment
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux

# 3. Install dependencies
pip install -r requirements.txt

# 4. Add your Groq API key
# Create a file named .env in the project root containing:
GROQ_API_KEY=gsk_your_actual_key_here
```

---

## Running the pipeline

From inside the `src/` folder:

```bash
cd src

# Topic-only input
python main.py --input "Why we procrastinate, and how to stop"

# Source URL input
python main.py --input "https://www.ted.com/talks/julian_treasure_how_to_speak_so_that_people_want_to_listen"
```

The final video is written to `outputs/final_<run_id>.mp4`, where `<run_id>`
is a short hash derived from the input text.

### Optional: force re-running specific stages

Each stage is cached by input hash. To force a stage to re-run even if
cached (useful when iterating on one stage):

```bash
python main.py --input "..." --force storyboard assets
```

Valid stage names: `gather_info`, `storyboard`, `assets`, `assembly`.

---

## Exact commands used to produce the submitted videos

```bash
python main.py --input "Why we procrastinate, and how to stop"
python main.py --input "https://www.ted.com/talks/julian_treasure_how_to_speak_so_that_people_want_to_listen"
```

Both runs used a completely empty `cache/` directory (no prior cached
results), so these commands reproduce the submitted videos from scratch,
top to bottom, through every real stage.

---

## Project structure

```
topic2video/
├── src/
│   ├── orchestrator.py      # Task graph engine: caching + retries
│   ├── pipeline_stages.py   # The 4 real pipeline stages
│   └── main.py               # Single CLI entry point
├── notebooks/
│   └── topic_to_video.ipynb  # Same pipeline, notebook form
├── cache/                    # Per-stage cached results (safe to delete)
│   ├── gather_info/
│   ├── storyboard/
│   ├── assets/
│   └── final/
├── outputs/
│   ├── final_<id>.mp4        # Final rendered videos
│   └── scene_assets/<id>/    # Per-scene images/audio/motion clips
├── APPROACH.md                # Design write-up
├── README.md                  # This file
├── requirements.txt
└── .env                       # Your Groq API key (not included — see Setup)
```

---

## Dependencies

See `requirements.txt` for exact versions. Key libraries:

| Library | Purpose |
|---|---|
| `groq` | LLM calls (research, storyboard generation) |
| `trafilatura` | Article text extraction from URLs |
| `youtube-transcript-api` | Transcript fetching for YouTube/TED content |
| `edge-tts` | Free text-to-speech narration |
| `mutagen` | Measuring real audio clip duration |
| `moviepy` | Video assembly: Ken Burns motion, cross-fades, captions, resize |
| `Pillow` / `numpy` | Frame-level image processing for Ken Burns rendering |
| `python-dotenv` | Loading the Groq API key from `.env` |
| `requests` | HTTP calls (article fetch, image generation, YouTube search) |

Image generation (Pollinations.ai) and narration (`edge-tts`) require no
API keys. Only the Groq key is needed.

---

## Notes on runtime

A full fresh run (no cache) takes roughly 5–10 minutes, dominated by real
image, audio, and video generation for each scene (6–7 scenes per video).
Re-running identical input completes in under a second per stage, since
every stage's result is cached by input hash — this is demonstrated
directly in the terminal output (`cached=True`, `0.00s` per stage on a
repeat run).
