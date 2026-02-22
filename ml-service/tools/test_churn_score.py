#!/usr/bin/env python3
"""
AlephBERT Churn Score Tester (Pure Multi-Prototype)

Tests churn detection using multiple prototype embeddings — NO keyword dependency.
Shows per-prototype similarity scores for calibration.

Usage:
    python test_churn_score.py "לקוח רוצה לעבור לגולן"
    python test_churn_score.py --interactive
    python test_churn_score.py --file summaries.txt
    python test_churn_score.py --config-only
"""

import os
import sys
import json
import argparse
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

# Add parent directories to path for imports
SCRIPT_DIR = Path(__file__).resolve().parent
ML_SERVICE_DIR = SCRIPT_DIR.parent
sys.path.insert(0, str(ML_SERVICE_DIR / 'src'))

# Lazy imports to handle missing dependencies gracefully
np = None
torch = None
SentenceTransformer = None


def check_dependencies():
    """Check if required dependencies are available."""
    missing = []
    try:
        import numpy
    except ImportError:
        missing.append('numpy')
    try:
        import torch
    except ImportError:
        missing.append('torch')
    try:
        import sentence_transformers
    except ImportError:
        missing.append('sentence-transformers')

    if missing:
        print("\033[91mError: Missing required dependencies:\033[0m")
        for dep in missing:
            print(f"  - {dep}")
        print("\n\033[93mTo install, run:\033[0m")
        print(f"  pip install {' '.join(missing)}")
        print("\n\033[93mOr run inside the ml-service Docker container:\033[0m")
        print("  docker exec -it call-analytics-ml python tools/test_churn_score.py ...")
        return False
    return True


def load_dependencies():
    """Load heavy dependencies only when needed."""
    global np, torch, SentenceTransformer
    if np is None:
        import numpy as _np
        np = _np
    if torch is None:
        import torch as _torch
        torch = _torch
    if SentenceTransformer is None:
        from sentence_transformers import SentenceTransformer as _ST
        SentenceTransformer = _ST


# ANSI color codes for beautiful output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    DIM = '\033[2m'
    RESET = '\033[0m'


def colorize(text: str, color: str) -> str:
    """Apply ANSI color to text."""
    return f"{color}{text}{Colors.RESET}"


class ChurnScoreTester:
    """
    Standalone churn score tester using AlephBERT embeddings.

    Uses multiple prototype embeddings for churn detection — no keywords.
    Replicates the exact logic from embedding_classifier.py.
    """

    def __init__(self, config_dir: Optional[Path] = None):
        self.config_dir = config_dir or (ML_SERVICE_DIR / 'config')
        self.model = None
        self.churn_embeddings: List = []
        self.churn_description = ""
        self.churn_prototypes: List[str] = []
        self.churn_threshold = 40
        self.prototype_labels: List[str] = []

        # Rescaling boundaries (should match embedding_classifier.py)
        self.min_baseline = 0.50
        self.max_signal = 0.82

    def load_configs(self) -> bool:
        """Load classification configurations (churn prototypes)."""
        try:
            classifications_path = self.config_dir / 'call-classifications.json'
            with open(classifications_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            churn_config = data.get('churn_detection', {})
            if not churn_config.get('enabled', False):
                print(colorize("Warning: Churn detection is disabled in config", Colors.YELLOW))
                return False

            self.churn_description = churn_config.get('description', '')
            self.churn_prototypes = churn_config.get('churn_prototypes', [])
            self.churn_threshold = int(churn_config.get('threshold', 40))

            # Build labels for display (prototypes only, no description)
            self.prototype_labels = [f'proto_{i}' for i in range(len(self.churn_prototypes))]

            print(f"  Loaded {colorize(str(len(self.churn_prototypes)), Colors.GREEN)} churn prototypes (threshold: {self.churn_threshold})")

            return True

        except Exception as e:
            print(colorize(f"Error loading configs: {e}", Colors.RED))
            return False

    def load_model(self) -> bool:
        """Load the AlephBERT model."""
        try:
            load_dependencies()

            print(colorize("\nLoading AlephBERT model...", Colors.CYAN))

            # Determine device
            device = 'cuda' if torch.cuda.is_available() else 'cpu'
            print(f"  Device: {colorize(device, Colors.GREEN)}")

            # Set offline mode
            os.environ['HF_HUB_OFFLINE'] = '1'
            os.environ['TRANSFORMERS_OFFLINE'] = '1'

            # Try different cache locations
            cache_paths = [
                '/app/cache/huggingface',
                os.path.expanduser('~/.cache/huggingface'),
                './cache/huggingface',
                str(ML_SERVICE_DIR / 'cache' / 'huggingface')
            ]

            model_name = 'imvladikon/sentence-transformers-alephbert'

            for cache_path in cache_paths:
                if os.path.exists(cache_path):
                    try:
                        print(f"  Trying cache: {cache_path}")
                        self.model = SentenceTransformer(
                            model_name,
                            device=device,
                            cache_folder=cache_path
                        )
                        print(colorize("  Model loaded successfully!", Colors.GREEN))
                        break
                    except Exception:
                        continue

            if self.model is None:
                # Try without specific cache folder (will download if needed)
                print("  Attempting to load model (may download if not cached)...")
                self.model = SentenceTransformer(model_name, device=device)

            # Verify model
            test_embedding = self.model.encode("test", convert_to_numpy=True)
            print(f"  Embedding dimension: {colorize(str(len(test_embedding)), Colors.GREEN)}")

            return True

        except Exception as e:
            print(colorize(f"Error loading model: {e}", Colors.RED))
            return False

    def _encode_and_normalize(self, text: str):
        """Encode text and L2-normalize the embedding."""
        embedding = self.model.encode(text, convert_to_numpy=True)
        return embedding / np.linalg.norm(embedding)

    def compute_churn_embeddings(self) -> bool:
        """Compute all churn prototype embeddings."""
        try:
            print(colorize("\nComputing churn prototype embeddings (no description)...", Colors.CYAN))
            self.churn_embeddings = []

            # Only embed focused prototypes — description is too generic
            for i, prototype in enumerate(self.churn_prototypes):
                emb = self._encode_and_normalize(prototype)
                self.churn_embeddings.append(emb)
                print(f"  [{i}] prototype: {prototype[:60]}...")

            print(colorize(f"  Total: {len(self.churn_embeddings)} embeddings", Colors.GREEN))
            return True

        except Exception as e:
            print(colorize(f"Error computing churn embeddings: {e}", Colors.RED))
            return False

    def score_text(self, text: str) -> Dict:
        """
        Score a text for churn probability using multi-prototype similarity.

        Args:
            text: The call summary or transcription

        Returns:
            Dict with full scoring breakdown
        """
        start_time = datetime.now()

        # Generate text embedding
        text_embedding = self._encode_and_normalize(text)

        # Compute similarity against each prototype
        prototype_similarities = []
        for churn_emb in self.churn_embeddings:
            sim = float(np.dot(text_embedding, churn_emb))
            prototype_similarities.append(round(sim, 4))

        # Best match
        best_sim = max(prototype_similarities)
        best_idx = prototype_similarities.index(best_sim)

        # Rescale to 0-100
        normalized = (best_sim - self.min_baseline) / (self.max_signal - self.min_baseline)
        final_score = max(0, min(100, normalized * 100))

        # Determine churn status
        is_churn = final_score >= self.churn_threshold

        elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000

        return {
            'text': text,
            'is_churn': is_churn,
            'final_score': int(final_score),
            'raw_similarity': round(best_sim, 4),
            'best_prototype_index': best_idx,
            'best_prototype_label': self.prototype_labels[best_idx] if best_idx < len(self.prototype_labels) else f'[{best_idx}]',
            'prototype_scores': prototype_similarities,
            'processing_time_ms': round(elapsed_ms, 1),
            'thresholds': {
                'min_baseline': self.min_baseline,
                'max_signal': self.max_signal,
                'churn_threshold': self.churn_threshold
            }
        }

    def print_result(self, result: Dict):
        """Print a beautifully formatted result."""
        print("\n" + "=" * 70)
        print(colorize("  CHURN SCORE ANALYSIS (Pure AlephBERT)", Colors.BOLD))
        print("=" * 70)

        # Input text (truncated if long)
        text = result['text']
        if len(text) > 100:
            display_text = text[:97] + "..."
        else:
            display_text = text
        print(f"\n{colorize('Input:', Colors.CYAN)} {display_text}")

        # Prototype similarity breakdown
        print(f"\n{colorize('Prototype Similarities:', Colors.CYAN)}")
        print("-" * 50)

        proto_scores = result['prototype_scores']
        best_idx = result['best_prototype_index']

        for i, sim in enumerate(proto_scores):
            label = self.prototype_labels[i] if i < len(self.prototype_labels) else f'[{i}]'
            # Visual bar (scaled: 0.4-0.9 range mapped to 0-30 chars)
            bar_val = max(0, min(30, int((sim - 0.4) / 0.5 * 30)))
            is_best = (i == best_idx)

            if is_best:
                marker = colorize(" << BEST", Colors.YELLOW + Colors.BOLD)
                bar_color = Colors.YELLOW
            else:
                marker = ""
                bar_color = Colors.BLUE

            bar = colorize("█" * bar_val, bar_color) + "░" * (30 - bar_val)
            sim_str = f"{sim:.4f}"
            print(f"  [{i}] {label:<14} {sim_str}  [{bar}]{marker}")

        # Best similarity
        best_sim = result['raw_similarity']
        print(f"\n{colorize('Best Similarity:', Colors.CYAN)}  {colorize(f'{best_sim:.4f}', Colors.BLUE)} (prototype: {result['best_prototype_label']})")
        print(f"  Rescale range:       [{self.min_baseline:.2f} - {self.max_signal:.2f}]")

        # Final score
        final = result['final_score']
        is_churn = result['is_churn']

        print(f"\n{'-' * 50}")

        if is_churn:
            final_color = Colors.RED
            status = "CHURN RISK"
            status_color = Colors.RED
        else:
            final_color = Colors.GREEN
            status = "NO CHURN"
            status_color = Colors.GREEN

        # Final score bar
        bar_len = int(final / 2)
        threshold_pos = int(self.churn_threshold / 2)
        bar_chars = []
        for i in range(50):
            if i == threshold_pos:
                bar_chars.append(colorize("│", Colors.YELLOW))
            elif i < bar_len:
                if final >= self.churn_threshold:
                    bar_chars.append(colorize("█", Colors.RED))
                else:
                    bar_chars.append(colorize("█", Colors.GREEN))
            else:
                bar_chars.append("░")
        bar = "".join(bar_chars)

        print(f"\n  {colorize('FINAL SCORE:', Colors.BOLD)}      {colorize(str(final), final_color + Colors.BOLD)}")
        print(f"  [{bar}]")
        print(f"  {' ' * (threshold_pos)}↑")
        print(f"  {' ' * max(0, threshold_pos - 5)}threshold={self.churn_threshold}")

        print(f"\n  {colorize('STATUS:', Colors.BOLD)}           {colorize(status, status_color + Colors.BOLD)}")

        # Processing time
        print(f"\n  {colorize('Processing:', Colors.DIM)} {result['processing_time_ms']:.1f}ms")

        print("\n" + "=" * 70)


def show_config_only(args):
    """Show configuration without loading the model (no dependencies required)."""
    config_dir = Path(args.config_dir) if args.config_dir else (ML_SERVICE_DIR / 'config')

    print(colorize("\n" + "=" * 70, Colors.CYAN))
    print(colorize("  Churn Detection Configuration (Pure AlephBERT)", Colors.BOLD + Colors.CYAN))
    print(colorize("=" * 70, Colors.CYAN))

    try:
        classifications_path = config_dir / 'call-classifications.json'
        with open(classifications_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        churn_config = data.get('churn_detection', {})

        print(f"\n{colorize('Churn Detection Status:', Colors.YELLOW)}")
        enabled = churn_config.get('enabled', False)
        status_color = Colors.GREEN if enabled else Colors.RED
        print(f"  Enabled:    {colorize(str(enabled), status_color)}")
        print(f"  Threshold:  {colorize(str(churn_config.get('threshold', 40)), Colors.BLUE)}")

        # Description
        print(f"\n{colorize('Description (embedding source):', Colors.YELLOW)}")
        print("-" * 50)
        description = churn_config.get('description', 'Not configured')
        words = description.split()
        line = ""
        for word in words:
            if len(line) + len(word) + 1 > 65:
                print(f"  {line}")
                line = word
            else:
                line = f"{line} {word}".strip()
        if line:
            print(f"  {line}")
        print("-" * 50)

        # Prototypes
        prototypes = churn_config.get('churn_prototypes', [])
        print(f"\n{colorize(f'Churn Prototypes ({len(prototypes)}):', Colors.YELLOW)}")
        proto_names = [
            "Porting / churn signal",
            "Permanent disconnect / no use",
            "Price/service dissatisfaction",
            "Technical frustration + churn intent",
            "Considering leaving / got better offer"
        ]
        for i, proto in enumerate(prototypes):
            name = proto_names[i] if i < len(proto_names) else f"Prototype {i}"
            print(f"\n  {colorize(f'[{i}] {name}:', Colors.BLUE)}")
            # Wrap text
            words = proto.split()
            line = "    "
            for word in words:
                if len(line) + len(word) + 1 > 68:
                    print(line)
                    line = "    " + word
                else:
                    line = f"{line} {word}".strip() if line.strip() else f"    {word}"
            if line.strip():
                print(line)

        # Scoring formula
        print(f"\n{colorize('Scoring Formula (Pure Embedding):', Colors.YELLOW)}")
        print("  1. Generate embedding for input text using AlephBERT")
        print("  2. Compute cosine similarity with EACH churn prototype")
        print("  3. Take the MAX similarity across all prototypes")
        print("  4. Rescale: score = (max_sim - 0.50) / (0.82 - 0.50) * 100")
        print(f"  5. {colorize('CHURN', Colors.RED)} if score >= {churn_config.get('threshold', 40)}")
        print(f"\n  {colorize('No keyword boost — pure semantic similarity', Colors.GREEN)}")

    except Exception as e:
        print(colorize(f"Error loading config: {e}", Colors.RED))

    print("\n" + "=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Test AlephBERT churn scoring (pure multi-prototype, no keywords)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s "הלקוח רוצה לעבור לגולן כי המחירים יקרים"
  %(prog)s --interactive
  %(prog)s --file summaries.txt
  %(prog)s --config-only
        """
    )
    parser.add_argument('text', nargs='?', help='Text to analyze')
    parser.add_argument('-i', '--interactive', action='store_true',
                        help='Interactive mode for multiple tests')
    parser.add_argument('-f', '--file', help='File with texts to analyze (one per line)')
    parser.add_argument('--config-dir', help='Path to config directory')
    parser.add_argument('--show-churn-desc', action='store_true',
                        help='Show the churn description used for embedding')
    parser.add_argument('--config-only', action='store_true',
                        help='Show configuration without loading model (no deps required)')

    args = parser.parse_args()

    # Config-only mode doesn't require heavy dependencies
    if args.config_only:
        show_config_only(args)
        return

    # Check dependencies before proceeding
    if not check_dependencies():
        sys.exit(1)

    # Load dependencies
    load_dependencies()

    # Initialize tester
    config_dir = Path(args.config_dir) if args.config_dir else None
    tester = ChurnScoreTester(config_dir)

    # Load configs
    print(colorize("\n" + "=" * 70, Colors.CYAN))
    print(colorize("  AlephBERT Churn Score Tester (Pure Multi-Prototype)", Colors.BOLD + Colors.CYAN))
    print(colorize("=" * 70, Colors.CYAN))

    if not tester.load_configs():
        sys.exit(1)

    # Show churn description if requested
    if args.show_churn_desc:
        print(f"\n{colorize('Churn Description:', Colors.YELLOW)}")
        print("-" * 50)
        print(tester.churn_description)
        print("-" * 50)
        print(f"\n{colorize('Prototypes:', Colors.YELLOW)}")
        for i, proto in enumerate(tester.churn_prototypes):
            print(f"  [{i}] {proto[:80]}...")

    # Load model
    if not tester.load_model():
        sys.exit(1)

    # Compute churn embeddings (multi-prototype)
    if not tester.compute_churn_embeddings():
        sys.exit(1)

    # Process input
    if args.interactive:
        print(f"\n{colorize('Interactive Mode', Colors.CYAN)} - Enter text to analyze (Ctrl+C to exit)")
        print("-" * 50)
        while True:
            try:
                text = input(f"\n{colorize('>', Colors.GREEN)} ").strip()
                if not text:
                    continue
                if text.lower() in ['quit', 'exit', 'q']:
                    break
                result = tester.score_text(text)
                tester.print_result(result)
            except KeyboardInterrupt:
                print("\n\nGoodbye!")
                break
            except Exception as e:
                print(colorize(f"Error: {e}", Colors.RED))

    elif args.file:
        file_path = Path(args.file)
        if not file_path.exists():
            print(colorize(f"File not found: {args.file}", Colors.RED))
            sys.exit(1)

        with open(file_path, 'r', encoding='utf-8') as f:
            lines = [line.strip() for line in f if line.strip()]

        print(f"\n{colorize(f'Processing {len(lines)} texts...', Colors.CYAN)}")

        churn_count = 0
        for text in lines:
            result = tester.score_text(text)
            tester.print_result(result)
            if result['is_churn']:
                churn_count += 1

        print(f"\n{colorize('Summary:', Colors.BOLD)}")
        print(f"  Total: {len(lines)}")
        print(f"  Churn: {colorize(str(churn_count), Colors.RED)} ({churn_count/len(lines)*100:.1f}%)")
        print(f"  Clean: {colorize(str(len(lines) - churn_count), Colors.GREEN)}")

    elif args.text:
        result = tester.score_text(args.text)
        tester.print_result(result)

    else:
        # Demo mode with examples
        print(f"\n{colorize('Demo Mode', Colors.CYAN)} - Showing example analyses")
        print("-" * 50)

        examples = [
            "הלקוח רוצה לעבור לגולן כי המחירים יקרים מדי",
            "לקוח בירר על חיוב בחשבונית והבין את הסכום",
            "הלקוח לא מרוצה מהשירות ושוקל לעזוב",
            "לקוח חדש רוצה להצטרף לפלאפון מסלקום",
            "הלקוח שבע רצון מהפתרון שקיבל, תודה רבה",
            "בעיות קליטה חוזרות, הלקוח מתוסכל ורוצה לנתק",
            "הלקוח אין לו שימוש בקו ורוצה לבטל אותו",
            "קיבלתי הצעה טובה יותר מסלקום ובודק אם כדאי לעבור",
        ]

        for text in examples:
            result = tester.score_text(text)
            tester.print_result(result)
            print()


if __name__ == '__main__':
    main()
