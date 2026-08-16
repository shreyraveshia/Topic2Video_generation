"""
orchestrator.py
================
The engine that runs our task graph: caches results, retries failures.
"""
from __future__ import annotations

"""
orchestrator.py
================

This is the "engine" of the pipeline. It is NOT tied to video generation at
all -- it is a small, general-purpose task graph runner that gives us three
things the assignment explicitly asks for:

1. Tasks are independent units with explicit inputs/outputs (a "task graph",
   not a single linear script).
2. Each task is independently RETRYABLE -- if a task fails (e.g. an API
   timeout), we retry it a few times with backoff before giving up, and we
   never re-run tasks that already succeeded.
3. Each task's output is CACHED to disk, keyed by a hash of its inputs. If
   you run the pipeline again with the same input, already-completed tasks
   are skipped entirely and their cached result is reused instantly.

Why a hand-rolled orchestrator instead of a library like Prefect/Airflow?
--------------------------------------------------------------------------
For a 4-stage linear pipeline like ours, a full workflow engine is more
machinery than the problem needs, and it adds a heavy dependency + learning
curve that isn't necessary to demonstrate the design ideas being evaluated
here (retry-safety, caching, explicit task boundaries). This module
implements those same ideas directly and transparently in ~150 lines, which
also makes it easier to read/audit for this take-home. The design mirrors
what a library like Prefect would give you (a `@task` decorator, automatic
caching, automatic retries) -- if this pipeline needed to scale to many
concurrent pipelines / distributed workers / a dashboard, migrating to
Prefect would be a natural next step, and because every task is a plain
Python function with a clear input/output contract, that migration would be
mechanical, not a redesign.
"""


import functools
import hashlib # (for turning data into cache-key strings)
import json    # (saving/loading structured data to files),
import logging # (printing nicely-formatted status messages — better than plain print())
import time    # (for the retry delays)
from dataclasses import dataclass, field # dataclass-(a shortcut for creating simple data-holding classes).
from pathlib import Path   # (a clean way to handle file paths across Windows/Mac/Linux)
from typing import Any, Callable

logging.basicConfig(
    level=logging.INFO,  # We want INFO-level messages and above. So things like: ```python logger.info(...), logger.warning(...), logger.error(...), will be visible.
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
) # The `-7s` means format the string to a width of 7 characters.


# Create our logger
logger = logging.getLogger("orchestrator") # This creates a logger named: orchestrator


class TaskFailedError(RuntimeError):
    """Raised when a task exhausts all of its retries."""

'''
TaskFailedError — a custom exception. Python has built-in errors like ValueError, 
but we're defining our own specifically for "this task tried its retries and still failed," 
so later error messages are clearer about what actually went wrong.
'''


def _stable_hash(obj: Any) -> str:

    # _stable_hash(obj) :- This is the function that makes caching actually work
    """
    Turn any JSON-serializable object into a short, stable hash string.
     :: This is how we key our cache: same inputs -> same hash -> same cache file.


    This is how we key our cache: the hash is computed from a task's
    *inputs*, so identical inputs always map to the same cache file,
    and changed inputs automatically produce a different (fresh) cache
    entry. We don't need to manually invalidate anything.
    """
    payload = json.dumps(obj, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


    # Let's trace through it with a real example.:-
    # Say obj is {"query": "Why we procrastinate"}.
    # json.dumps(obj, sort_keys=True, ...) turns that Python dictionary into a 
    # plain text string: '{"query": "Why we procrastinate"}'
    # sort_keys=True matters — it guarantees that if the same data comes in with keys in a different order, it still produces the exact same string (dictionaries in Python don't guarantee order, so without this, the same data could hash differently by accident).
    # .encode("utf-8"):- turns that text string into raw bytes, because the next step (hashing) needs bytes, not text.
    # hashlib.sha256(payload):- runs that byte data through the SHA-256 algorithm — a one-way math function that takes any input and produces a fixed-length scrambled fingerprint. Same input always produces the exact same fingerprint; different input (even by one character) produces a totally different one.
    # .hexdigest():- converts that fingerprint into a readable string of letters/numbers (like a3f9c81b2e...).
    # [:16] just keeps the first 16 characters — full SHA-256 hashes are 64 characters, way more than we need for a filename; 16 is plenty unique for our purposes.
''' 
Why does this matter? 

This is literally how "cache this task's output, and reuse it next time" works: 
we turn the task's input into a short unique code, and use that code as the filename to save/check results under.
Same input → same filename → we find the old result. 
Different input → different filename → nothing found, so we actually run the task.
'''


# @dataclass:- is a Python shortcut — instead of writing a full class with an __init__ method by hand,
# this one decorator auto-generates it for you based on the fields listed.

'''
TaskResult:- 

is just a small labeled container we'll use to carry back 4 pieces of info every time a task finishes: 

its name, its output (the actual data it produced), 
whether it from_cache (True/False — did we skip real work and reuse an old result?), and how long it took (duration_s).
Nothing clever happening here — just a clean, consistent "report card" shape every task hands back.
'''

@dataclass
class TaskResult: # (This creates a data class. Its job is to standardize what the orchestrator returns after running a task.)
    """Standard envelope every task returns, so the runner can log/cache uniformly."""
    name: str # Task name
    output: Any # The actual result produced by the task
    from_cache: bool # True- means  We didn't execute the task; we loaded its previous result from cache
                     # False- means We executed the task
    duration_s: float # How many seconds the task took.

# So a `TaskResult` looks conceptually like:
'''
TaskResult(
    name="assets",
    output={...},
    from_cache=False,
    duration_s=3.21
)
'''

# this is the core class that does the actual caching-check and retry-loop work. 

class CachedRetryableTask:
    """
    Wraps a plain Python function (the "task body") with caching + retry
    behaviour. 

    Each task in our pipeline (gather_info, storyboard, assets, assembly) is built with this wrapper.

    Parameters
    ----------
    name:
        Human-readable task name, used for logging and as the cache
        sub-folder.
    func:
        The actual task logic. Must accept a single dict of inputs and
        return a JSON-serializable dict (or, for binary outputs like
        images/audio/video, a dict of *file paths* pointing at binary
        artifacts already written to disk by the task itself).
    cache_dir:
        Root folder where this task's cached outputs are stored.
    max_retries:
        How many times to retry the task body if it (raises an exception/before giving up).
    backoff_s:
        how many seconds to wait between attempts.
        Base delay between retries (grows linearly: backoff_s * attempt).
    """


    
# This is the constructor. Whenever we create: CachedRetryableTask(...), Python executes this.

# This runs once, when we first create a task object (like CachedRetryableTask("gather_info", gather_info_func, cache_dir=..., ...)). 

# It just stores everything we're told:

# self.name — a label, like "gather_info", used in logs so we know which task is talking

# self.func — the actual Python function containing the real logic (e.g., the function that calls Groq). 
# Notice: this class doesn't know or care what the function does — it just knows "I have some function to call." 
# That's the whole trick that lets us swap dummy logic for real logic later without touching this file.

# self.cache_dir - which folder this task's results get saved into (e.g., cache/gather_info/) 

# self.cache_dir.mkdir(parents=True, exist_ok=True) — actually create that folder on disk right now if it doesn't already exist. 
# parents=True:- means "create parent folders too if needed,"   exist_ok=True:- means "don't error
    def __init__( 
        self,
        name: str,
        func: Callable[[dict], dict],
        cache_dir: Path,
        max_retries: int = 3,
        backoff_s: float = 2.0,
    ):
        self.name = name
        self.func = func # Store function - Now the object knows which function it should execute.
        self.cache_dir = Path(cache_dir) # cache_dir:- Where cache files should be stored.
        self.cache_dir.mkdir(parents=True, exist_ok=True) # actually create that folder on disk right now if it doesn't already exist. parents=True means "create parent folders too if needed," exist_ok=True means "don't error out if the folder's already there."
        self.max_retries = max_retries
        self.backoff_s = backoff_s # Base retry delay is 2 seconds.


# _cache_path():- This determines:
# > **Where should the cached result for these inputs be stored?**
    def _cache_path(self, inputs: dict) -> Path:
        key = _stable_hash(inputs)            # inputs -> stable_hash -> "a82f91c3e..."
        return self.cache_dir / f"{key}.json" # Creates the path. Ex- cache/assets/a82f91c3e8d4ab12.json

# This is a small helper method. Give it the task's inputs (a dictionary), and it:
# 1. Calls our '_stable_hash()' function from before to turn those inputs into a short unique code
# 2. Builds a full file path like cache/gather_info/a3f9c81b2e4d7f01.json


    
# `.run()` --->Now the most important method.
# This means: -> Execute this task using these inputs.  It returns TakeResult.
    def run(self, inputs: dict, force: bool = False) -> TaskResult: 
        cache_path = self._cache_path(inputs)  # Determine where the result *would* be cached.
        start = time.time() # `time.time()` gives the current time. to determine how long the task took.

        # 1. Check cache first (unless force=True is explicitly requested).
        if not force and cache_path.exists(): # This means: -> If the user didn't explicitly force a rerun **AND** the cache file exists... then use the cache.

            logger.info("[%s] cache HIT (%s) -- skipping re-run", self.name, cache_path.name) #[gather_info] cache HIT (abc123.json) -- skipping re-run
            cached = json.loads(cache_path.read_text())

# `cache_path.exists()` ->Checks whether the file exists.
# cached = json.loads(cache_path.read_text()) -> This does 2 operations:-

# `cache_path.read_text()` ->Reads the JSON file as text.
# `json.loads(...)` -> Converts JSON text → Python object. Turns that text back into a Python dictionary (or list, or whatever the original output was).

            return TaskResult(
                name=self.name,
                output=cached,
                from_cache=True,
                duration_s=time.time() - start,
            )
# if not force and cache_path.exists():-

# Before doing any real work, 
# we ask: "does a file already exist at this exact cache path?" 
# (Remember — that path is built from a hash of the inputs, so it only exists if we've run this exact task with these exact inputs before.)
# If yes (and we're not forcing a fresh run), 
# we just read that old saved result off disk with json.loads(cache_path.read_text()) and hand it back immediately — no real work happens, 
# this is instant. This is the "0.00s"

# We're telling the caller:
# Task name = gather_info
# Output = cached result
# from_cache = True
# Duration = how long reading cache took


        # 2. Not cached (or forced) -> run with retries.
        last_exc: Exception | None = None
# This creates a variable to remember the last exception.
# The type means :- Exception or None. This is just a type hint for clarity; Python doesn't enforce it at runtime.
# Initially : None - bcoz nothing has failed yet.


        for attempt in range(1, self.max_retries + 1):
            try: # Everything inside this block is monitored for exceptions.
                logger.info("[%s] attempt %d/%d ...", self.name, attempt, self.max_retries)
# Execute the actual task
                output = self.func(inputs)
                # This is the line where the actual task function runs.*
                # ex:-
                # if: self.func = gather_info,
                # then this effectively does: output = gather_info(inputs)
# The orchestrator itself doesn't know what `gather_info` does.
# That's an important design principle.
# Cache the successful output
                cache_path.write_text(json.dumps(output, indent=2, default=str))

# `json.dumps(output, indent=2, default=str)` :-> 
# 'json.dumps(output)':-Converts the Python object(Dictionary) back into JSON text. 
# `indent=2` makes it pretty-printed with 2 spaces of indentation. 
# `default=str` means "if there's any non-JSON-serializable object, just convert it to a string instead of failing."

# `cache_path.write_text(...)` -> Writes that JSON text to the cache file we determined earlier. This is how we save the result for future runs.

                logger.info("[%s] succeeded, cached -> %s", self.name, cache_path.name) # (print<- is same as->logger)
# ex- [gather_info] succeeded, cached -> abc123.json

                return TaskResult( # success — exit immediately
                    name=self.name,
                    output=output,
                    from_cache=False,  # because we actually executed the function.
                    duration_s=time.time() - start,
                )
# If nothing was cached, we actually call the real function (self.func(inputs) — this is where
# Groq/scraping/image-gen calls will eventually happen).
            
            except Exception as exc:  # noqa: BLE001 - we deliberately catch broadly here
# If anything inside `try` throws an exception, execution comes here.

                last_exc = exc  # Remember the error.
                logger.warning("[%s] attempt %d failed: %s", self.name, attempt, exc) # Log the failure.
                if attempt < self.max_retries: # If we're not on the final attempt, retry.
                    delay = self.backoff_s * attempt
                    logger.info("[%s] retrying in %.1fs ...", self.name, delay)
                    time.sleep(delay) # Pause execution.

# Wrapped in try/except: if it works, we save the result to the cache file and return success right away.
# If it throws any exception, we catch it, log a warning, 
# wait a bit (time.sleep(delay) — the wait grows longer each attempt, called "backoff"), 
# and loop around to try again — up to max_retries

        raise TaskFailedError(
            f"Task '{self.name}' failed after {self.max_retries} attempts"
        ) from last_exc # last_exc->It preserves the original exception as the **cause**.

# This creates our custom error.
# Ex:- Task 'assets' failed after 3 attempts
    
    # Only reached if every attempt failed. We raise our custom error, 
    # and from last_exc-> attaches the original error as context, so if you're debugging later, 
    # you can see exactly what the underlying failure actually was, not just "it failed."





# This class holds one thing: tasks, a list of CachedRetryableTask objects, in the order they should run.
# This says: -> The pipeline contains a list of `CachedRetryableTask` objects.
# Initially, create an empty list.

@dataclass
class Pipeline:
    """
    Runs a fixed sequence of CachedRetryableTask objects, passing each
    task's output forward as (part of) the next task's input. 
    
    This is our
    "task graph runner" -- deliberately simple since our graph is a
    straight line (gather_info -> storyboard -> assets -> assembly), but
    written so each stage is fully decoupled and independently testable.
    """

    tasks: list[CachedRetryableTask] = field(default_factory=list)
#field(default_factory=list)-> ensures each `Pipeline` gets its **own list**.




# field(default_factory=list) — a small Python quirk: you can't just write tasks: list = [] as a default in a dataclass 
# (this would cause a bug where all Pipeline objects secretly share the same list). 
# field(default_factory=list) tells Python "create a fresh, brand-new empty list for every new Pipeline object" — the correct safe way to do this.



    def add(self, task: CachedRetryableTask) -> "Pipeline": # Adds a task to the pipeline.
        self.tasks.append(task)                             # Adds it to the list.
        return self                                         # it returns the pipeline itself
# For example: [] ->[gather_info] -> [gather_info, storyboard] -> [gather_info, storyboard, assets]
   


#   run(self, initial_input, force=None) —>"""the heart of the whole orchestrator"""
# This is where the whole pipeline executes.
    def run(self, initial_input: dict, force: set[str] | None = None) -> dict[str, TaskResult]:
        """
        Executes every task in order. `initial_input` seeds the first task.
        Each subsequent task receives a dict containing the *original*
        initial_input plus every prior task's output, keyed by task name --
        so any task can access any earlier task's result if it needs to,
        not just the immediately preceding one.

        `force`: a set of task names to force-recompute even if cached
        (useful when debugging a single stage).
        """
        force = force or set()
        state: dict[str, Any] = {"initial_input": initial_input}
        results: dict[str, TaskResult] = {}

        for task in self.tasks:
            task_inputs = {**state}
            # We loop through each task in order.
            # {**state} makes a copy of the current state dictionary (the ** "unpacks" it into a new dict) — 
            # so each task gets to see everything done so far, but can't accidentally corrupt the shared original.


            result = task.run(task_inputs, force=(task.name in force))
            # This is where our `CachedRetryableTask.run()` method is called.
            # We actually run the task (this calls the run() method we built in the last message 
            # — cache-check, retry-loop, all of it), handing it everything known so far as its input.

            state[task.name] = result.output
            # This is the key line — once a task finishes, we add its output to state, labeled by that task's name.

            # So after gather_info finishes, state looks like -> 
            # { "initial_input": {...}, "gather_info": {...its output...} }


            # And when the next task (storyboard) runs, it receives this whole updated state as its input — meaning 
            # it can read inputs["gather_info"] to get what the first task produced. 
            # This is literally how data flows from one pipeline stage to the next.

            results[task.name] = result
            # Separately, we also keep a full record of every task's TaskResult (including timing/cache-hit info), 
            # so at the very end main.py can print a nice summary — this doesn't affect what tasks pass to each other, 
            # it's just bookkeeping for us.

            logger.info(
                "[%s] done in %.2fs (from_cache=%s)",
                task.name, result.duration_s, result.from_cache,
            )

        return results

    # We start a dictionary called state. 
    # Think of state as a shared notebook that gets a new page added every time a task finishes. 
    # Right now it only has one page: whatever input the user originally typed in (your topic/URL).








# > This function creates a decorator that converts a normal function into a `CachedRetryableTask`.
def task(name: str, cache_dir: Path, max_retries: int = 3, backoff_s: float = 2.0):
    """
    Convenience decorator: turns a plain function into a CachedRetryableTask.

        @task("gather_info", cache_dir=CACHE_ROOT / "gather_info")
        def gather_info(inputs: dict) -> dict:
            ...
            return {"content": "...", "source_type": "topic"}

    `gather_info` is now a CachedRetryableTask instance, ready to be added
    to a Pipeline.
    """
    def decorator(func: Callable[[dict], dict]) -> CachedRetryableTask:
        wrapped = functools.wraps(func)(func)
        return CachedRetryableTask(
            name=name, func=wrapped, cache_dir=cache_dir,
            max_retries=max_retries, backoff_s=backoff_s,
        )
    return decorator

''' a decorator — recognizable by the @ symbol we'll use with it (like @task("gather_info", ...)).
# ------
# A decorator is a function that wraps another function, 
# adding behavior to it without you having to rewrite that behavior every time.
'''

# Here's the problem it solves: without this, 
# every time we wanted to turn a plain function into a cached/retryable task, we'd have to write this manually:

# gather_info = CachedRetryableTask(name="gather_info", func=my_function, cache_dir=..., ...)

# That's clunky, especially with 4 tasks. 
# Instead, the decorator lets us write this, which reads much more naturally:

'''
@task("gather_info", cache_dir=CACHE_ROOT / "gather_info")
def gather_info(inputs: dict) -> dict:
    # real logic here
    return {...}
'''

'''
When Python sees @task(...) sitting right above a function definition, it automatically does this: 
take the function I just wrote (gather_info), pass it into task(...)'s inner decorator function, 
and replace gather_info with whatever that returns. So after this runs, 
gather_info is no longer a plain function — it's actually a full CachedRetryableTask object, 
ready to be handed straight to pipeline.add(gather_info).
'''
