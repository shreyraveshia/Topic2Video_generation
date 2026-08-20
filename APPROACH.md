# Topic-to-Video Generation — Approach & Design Document

## 1. Overview

This project implements a Python pipeline that takes either a plain-text topic
or a source URL (article, YouTube video, or TED talk) and produces a single
30–40 second, 1080×1920 (9:16) portrait video with narration, captions, and
visually consistent scenes.

The system is built as an **orchestrated task graph**, not a linear script,
per the assignment's explicit requirement. Each stage is independently
cacheable and retryable, so partial failures never force a full re-run.

Single entry point:
```bash
python main.py --input "Why we procrastinate, and how to stop"
python main.py --input "https://www.ted.com/talks/julian_treasure_how_to_speak_so_that_people_want_to_listen"
```

---

## 2. Task Graph & Orchestration

```
                     ┌────────────────┐
   user input  ───▶  │  gather_info   │
                     └───────┬────────┘
                             ▼
                     ┌────────────────┐
                     │   storyboard   │
                     └───────┬────────┘
                             ▼
                     ┌────────────────┐
                     │     assets     │
                     └───────┬────────┘
                             ▼
                     ┌────────────────┐
                     │    assembly    │
                     └───────┬────────┘
                             ▼
                      final_<id>.mp4
```

Each box is a `CachedRetryableTask`: a plain Python function wrapped with
automatic disk caching (keyed by a SHA-256 hash of its inputs) and automatic
retry-with-backoff (3 attempts, linear backoff) on failure. State flows
forward through a shared dictionary — each task can read any prior task's
output by name, not just the immediately preceding one.

**Why a hand-rolled orchestrator instead of Prefect/Airflow/Luigi?**
The pipeline is a small, fixed, linear graph (4 stages). A full workflow
engine adds a real dependency and learning-curve cost without adding
capability we needed for this scope. The custom orchestrator (`orchestrator.py`,
~150 lines) implements the same core ideas a library would give us — a
`@task` decorator, automatic caching, automatic retries — transparently and
auditably. If this pipeline needed to scale to many concurrent runs or
distributed workers, migrating to Prefect would be mechanical, since every
task is already a plain function with an explicit input/output contract.

**Proof of caching and retry behavior** (captured during real development,
not simulated for submission):
- Identical input on a second run: all 4 stages report `from_cache=True`,
  total time ~0.02s vs. several minutes on first run.
- A real library API change (`youtube_transcript_api`'s `get_transcript` →
  `.fetch()`) caused 3 clean, logged retry attempts before failing with a
  clear error — proving retry-then-fail behaves correctly, not just
  retry-then-succeed.

---

## 3. Stage-by-Stage Design

### 3.1 `gather_info`
Branches on input type:
- **Plain topic** → Groq LLM (`llama-3.3-70b-versatile`) asked to act as a
  careful researcher, instructed not to invent statistics or studies.
- **URL, TED talk** → TED's transcript view is JavaScript-rendered (confirmed
  by inspecting raw HTML — the transcript never appears in the initial
  response). Fix: scrape the talk's title (plain HTML, reliably present),
  search YouTube for `"<title> TED"`, fetch that video's real transcript.
- **URL, YouTube video** → direct transcript fetch via `youtube-transcript-api`.
- **URL, other article** → `trafilatura` extracts clean article text.

This layered fallback (TED-specific → YouTube-specific → generic article) is
deliberately general rather than hardcoded to the two PDF examples, so it
reasonably extends to similar inputs without claiming to solve arbitrary
web-content extraction.

### 3.2 `storyboard`
A single structured LLM call (not split into separate "selection" and
"writing" calls) that asks the model to: (1) select the 4–7 most
story-worthy points from the source, (2) write one narration line and one
visual description per scene, (3) generate one Visual Style Guide applied to
every scene for cross-scene consistency. One combined call was chosen over
two so selection and phrasing can't drift out of sync with each other.

For URL-sourced input, an explicit grounding instruction is added: narration
must not include facts, conclusions, or phrases not actually present in the
source. This was tightened mid-development after manual verification caught
one instance of an ungrounded addition — a generated line included a phrase
not actually present in its source transcript, later corrected.

Scene duration is fixed at 6.5–6.75s per scene. This is enforced by clamping
in code (`min(6.75, max(6.5, duration))`) rather than relying solely on the
prompt, since the LLM frequently returned durations outside the requested
range during testing.

### 3.3 `assets`
Per scene: an image is generated (Pollinations.ai, free, no API key) using
the visual description plus the shared style guide text, giving cross-scene
visual consistency. Narration audio is generated via `edge-tts` (free,
natural-sounding, no key). Real audio duration is then measured directly
from the rendered file (via `mutagen`) — this measured value, not the LLM's
predicted duration, is treated as ground truth for scene timing.

### 3.4 `assembly`
Each scene's still image becomes a moving clip via a **Ken Burns pan/zoom**
effect, rendered into a fixed-size output frame on every frame of the clip
(an earlier version resized the frame itself over time, which corrupted the
video — codecs require every frame to be an identical size). Clips are
joined with 0.3s cross-fade transitions.
Narration audio is composited at the exact start time of its scene using a
shared timeline calculation (the same function used to position both
video cross-fades and audio, so they cannot drift apart). Captions are
burned in per scene using the narration text. The whole video is scaled and
padded (not stretched) to exactly 1080×1920.

---

## 4. Real vs. AI-Generated Video Motion — Design Decision

The assignment notes that "most video models emit clips of only a few
seconds." Real AI video generation (image-to-video, via Hugging Face's
Inference Providers / fal-ai) was investigated as the primary motion
mechanism.

Given the 2-day timeline and free-tier-only constraint, Ken Burns motion was
adopted as the **sole, reliable** motion mechanism for the submission,
rather than building a fragile primary/fallback system around a
provider-compatibility question that wasn't yet fully resolved. This is a
deliberate scope decision, not an oversight — real AI-generated per-scene
motion remains a natural next step if development time allowed.

---

## 5. Real Bugs Found and Fixed During Development

Documented here because the debugging process itself demonstrates the
"handling of partial failures" and engineering judgment the assignment
evaluates — not just the final passing state.

1. **TED transcript page is JavaScript-rendered.** Raw HTML fetch returned
   only a "Read transcript" button, no content. Fixed by scraping the talk's
   title instead (present in plain HTML), searching YouTube for that title,
   and fetching the real video transcript from there.
2. **`youtube_transcript_api` breaking API change.** `get_transcript()` no
   longer exists in the installed version; replaced with instance-based
   `.fetch()`. Caught via 3 real logged retry attempts before a clear final
   error — a genuine demonstration of the retry system, not a staged one.
3. **Ken Burns frame-size corruption.** Naively resizing a clip frame-by-frame
   changes pixel dimensions over time, which video codecs don't support
   (every frame must be identical size) — produced visible diagonal-stripe
   corruption partway through the clip. Fixed by rendering into a
   fixed-size output canvas and cropping a growing virtual zoom window back
   down to constant dimensions each frame; also enforced even width/height
   (`libx264` requirement).
4. **Storyboard duration drift.** The LLM was asked for 5–6s scenes but
   regularly returned 7–8s. Rather than retrying indefinitely against a
   constraint we ourselves defined, duration is now deterministically
   clamped in code (`min(6, max(5, duration))`), reserving retries for
   genuinely non-deterministic failures.
5. **Real narration shorter than planned duration.** Measured TTS audio was
   consistently 3–4.5s against a 5–6s planned scene, leaving near-silent
   scenes and an under-length final video (~21–27s instead of 30–40s).
   Fixed with an enforced minimum scene duration, tuned against the actual
   measured cross-fade math to reliably land in the required range.
6. **Narration occasionally longer than its scene's video window.** Rather
   than letting audio bleed into the next scene's transition, the audio is
   regenerated at a proportionally faster `edge-tts` speaking rate (typically
   a few percent) so narration always completes within its own scene.
7. **Output resolution mismatch.** Source images from Pollinations.ai were
   not natively 1080×1920. Fixed by scaling proportionally and padding with
   black bars rather than stretching (which would distort the image).
8. **Caption clipping at the frame edge / ugly mid-word wraps on longer
   lines.** Fixed by moving caption position up and widening/resizing text
   parameters; a stricter word-boundary-safe wrapping approach (Python's
   `textwrap`) was also built and tested successfully, but was intentionally
   reverted in favor of the simpler built-in wrapping, accepting an
   occasional cosmetic mid-word hyphenation as a reasonable tradeoff.
9. **Ungrounded narration addition.** One generated line ("connection is
   key") was not actually present in its source transcript. Caught via
   manual line-by-line verification against the real transcript — not
   caught by automated word-count validation, since grounding is a semantic
   property, not a structural one. Fixed by strengthening the grounding
   prompt with an explicit self-check instruction; re-verified clean on a
   subsequent independent generation.
10. **Windows `ffprobe` PATH resolution.** An automated post-render
    validation step (duration/resolution/fps/audio-presence via ffprobe) was
    built and worked in isolation, but failed to locate `ffprobe` reliably
    via the `ffmpeg-python` wrapper on Windows. Given the manual ffprobe
    checks already confirm both submission videos meet every spec
    requirement, automatic invocation was reverted rather than spending
    further time on a platform-specific PATH issue this close to the
    deadline.

---

## 6. Assumptions & Limitations

- No paid API credits are used anywhere; every tool in the stack (Groq free
  tier, Pollinations.ai, edge-tts, trafilatura, youtube-transcript-api) is
  free with no cost ceiling relevant to this submission's scope.
- Topic-only research relies on the LLM's own knowledge rather than a live
  web search step; a production version would likely add real search for
  stronger factual grounding on topic-only inputs.
- Motion is Ken Burns pan/zoom on generated stills rather than genuine
  AI-generated video motion — a deliberate scope decision given the 2-day
  timeline and an unresolved provider-compatibility question with the
  image-to-video model tested, not an oversight.
- Captions are one full sentence per scene, timed to that scene's window,
  rather than word-level karaoke-style timing.
- Automated post-render validation (duration/resolution/fps/audio-presence
  via ffprobe) was built and verified working in isolation, but is not
  currently wired into the automatic pipeline run due to a Windows-specific
  issue locating the ffprobe binary through its Python wrapper. Manual
  ffprobe verification is provided instead for both submission videos.

## 7. Tech Stack Summary

| Stage | Tool | Why |
|---|---|---|
| Orchestration | Custom (`orchestrator.py`) | Small fixed graph; full framework unnecessary |
| Article scraping | `trafilatura` | Free, clean text extraction |
| Video/talk transcripts | `youtube-transcript-api` | Free, direct transcript access |
| Script/storyboard LLM | Groq (`""llama-3.3-70b-versatile""=> Got deprecated; so changing to :-> '''openai/gpt-oss-120b'''`) | Fast, genuinely free tier |
| Images | Pollinations.ai | Free, no key, reliable |
| Motion | Ken Burns (`moviepy`, custom frame renderer) | No GPU dependency, guaranteed to work |
| Narration | `edge-tts` | Free, no key, natural-sounding |
| Assembly | `moviepy` / FFmpeg | Industry standard; explicitly suggested by the assignment |
