from llama_cpp import Llama
import os

print("Initializing TinyLlama model for a more direct generation test...")

cpu_threads = os.cpu_count()
model_path = r"models\TinyLlama-1.1B-Chat-v1.0-GGUF\tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf"

# Initialize the model. We don't need 'chat_format' for this method.
llm = Llama(
    model_path=model_path,
    n_threads=cpu_threads,
    n_ctx=2048,
    verbose=False
)

print("Model initialized successfully.")

# --- THIS IS THE CRITICAL CHANGE ---
# We are manually building the prompt string exactly as TinyLlama was trained on.
# This special formatting is essential.
prompt = """<|system|>
You are a helpful assistant who writes short poems.</s>
<|user|>
Write a short poem about AI and coffee.</s>
<|assistant|>
"""

print("\n--- Sending manually formatted prompt directly to the model. ---")
print("--- This is the most reliable method. Please wait for 1-5 minutes. ---")

# We are now using the direct `llm()` call, not `create_chat_completion`.
response = llm(
    prompt,
    max_tokens=256,
    stop=["<|user|>", "</s>"],  # Tell the model when to stop talking
    echo=False  # This prevents the prompt from being repeated in the output
)

print("\n--- RESPONSE RECEIVED! ---")

# --- Now we extract the text from the response ---
print("\nGenerated Poem:")
try:
    # The response structure from this direct call is simpler
    content = response['choices'][0]['text']
    print(content.strip())
except (KeyError, IndexError, TypeError):
    print("[Error: Could not extract the poem from the model's response.]")
    print("\nFull raw response object for debugging:")
    print(response)