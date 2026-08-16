"""CLI entrypoint for topic-to-video pipeline.

The assignment explicitly requires a single entry point accepting the topic or URL. 

`main.py` is that entry point.
"""


"""
main.py
=======

Single entry point: python main.py --input "topic or URL"


This file's only job is to wire the 4 task-stages into a Pipeline and run
it against whatever input the user passed in. All real logic lives in
pipeline_stages.py; this file stays thin on purpose.
"""


import argparse
import sys

# argparse is Python's built-in tool for reading command-line arguments 
# — it's what lets you type --input "..." when running the script and have Python parse that cleanly
# used in- sys.exit(main())

from orchestrator import Pipeline # Import our Pipeline class.
from pipeline_stages import gather_info, generate_storyboard, generate_assets, assemble_video 
# Import all four tasks


def build_pipeline() -> Pipeline: # A function whose job is simply: -> Construct our pipeline.
    pipeline = Pipeline()         # Creates an empty pipeline. Initially -> pipeline.tasks = []
    pipeline.add(gather_info)
    pipeline.add(generate_storyboard)
    pipeline.add(generate_assets)
    pipeline.add(assemble_video)
    return pipeline               # Return the completed pipeline.

# build_pipeline()-> just lines our 4 tasks up in the correct order using the .add() chaining we built earlier.

def main(): # This is the main application function.

    parser = argparse.ArgumentParser(description="Topic/URL -> short vertical video generator")
    parser.add_argument("--input", required=True, help="A plain-text topic OR a source URL")
    #  This tells argpars -> The program requires a `--input` argument.
    # This satisfies the assignment's topic/URL input requirement. 

    parser.add_argument(
        "--force", nargs="*", default=[],   
        help="Task names to force re-run even if cached",
    )
    args = parser.parse_args()  # Now argparse looks at what the user typed.

# Build pipeline
    pipeline = build_pipeline()
# creats-> gather_info -> storyboard -> assets -> assembly


# Execute pipeline
    import hashlib
    run_id = hashlib.sha256(args.input.encode()).hexdigest()[:10]

    results = pipeline.run(
        initial_input={"query": args.input, "run_id": run_id},
        force=set(args.force),
    )
        # Why convert force to set? -> A set is useful because we repeatedly ask: task.name in force

# Why this approach: 
# we're generating a short, stable ID from the input itself (same technique as our cache-hashing from Session 1) 
# — so the same input always produces the same run_id, and different inputs get different folders automatically, 
# with zero extra bookkeeping needed.


    print("\n=== PIPELINE RUN SUMMARY ===")
    for name, result in results.items():
        # loops through: 
        # { 
        # "gather_info": TaskResult(...), 
        # "storyboard": TaskResult(...), ... 
        #}
        print(f"  {name:12s} | cached={result.from_cache!s:5s} | {result.duration_s:.2f}s")
        # `!s` means convert to string. ->Then width 5.


# # Get final result
    final = results["assembly"].output
    print(f"\nFinal video: {final['final_video_path']} ({final['num_scenes']} scenes)")


    # results["assembly"]` gives: TaskResult(...)
    # .output -> {
    # "final_video_path": "...",
    # "num_scenes": 4
    #  }

    # Final video: [DUMMY]/final_video.mp4 (4 scenes)
    '''
    
So output might look like:

=== PIPELINE RUN SUMMARY ===
  gather_info  | cached=False | 1.00s
  storyboard   | cached=False | 1.00s
  assets       | cached=False | 3.01s
  assembly     | cached=False | 1.00s

'''



''' What happens inside `pipeline.run()`?

This is the complete execution:

main.py
   │
   │ initial_input
   ↓
Pipeline.run()
   │
   ├── gather_info.run()
   │       │
   │       ├── check cache
   │       ├── execute if needed
   │       ├── retry if failure
   │       └── save result
   │
   ├── storyboard.run()
   │       │
   │       └── receives gather_info output
   │
   ├── assets.run()
   │       │
   │       └── receives storyboard output
   │
   └── assembly.run()
           │
           └── receives assets output
'''

if __name__ == "__main__":
    sys.exit(main())   # calls main()