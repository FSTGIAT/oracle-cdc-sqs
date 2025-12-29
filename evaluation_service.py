"""
Weekly ML Evaluation Service

Analyzes churned customers, generates RECOMMENDATIONS (not auto-applied).
Human must approve via dashboard before configs are updated.

This service runs as a weekly cron job on the CDC server (on-premise).
It queries Oracle for actual churn outcomes and compares with ML predictions.

Usage:
    python evaluation_service.py

Cron setup (run every Sunday at 8 AM):
    0 8 * * 0 cd /path/to/oracle-cdc-sqs && python evaluation_service.py >> /var/log/ml-evaluation.log 2>&1
"""
import os
import json
import logging
import re
from datetime import datetime
from typing import Dict, List, Optional, Any
from collections import Counter
from pathlib import Path
from dotenv import load_dotenv

# Load .env from the same directory as this file
ENV_PATH = Path(__file__).parent / '.env'
load_dotenv(ENV_PATH)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Try to import oracledb
try:
    import oracledb
    ORACLE_AVAILABLE = True
except ImportError:
    logger.warning("oracledb not installed - running in dry-run mode")
    ORACLE_AVAILABLE = False


class EvaluationService:
    """
    Weekly ML Evaluation Service

    Analyzes churned customers to evaluate ML prediction accuracy.
    Generates recommendations for human review (not auto-applied).
    """

    def __init__(self):
        """Initialize the evaluation service with Oracle connection config."""
        self.oracle_config = {
            'user': os.getenv('ORACLE_USER'),
            'password': os.getenv('ORACLE_PASSWORD'),
            'host': os.getenv('ORACLE_HOST'),
            'port': int(os.getenv('ORACLE_PORT', 1521)),
            'service_name': os.getenv('ORACLE_SERVICE_NAME', 'XE'),
        }

        # Churn detection thresholds
        self.high_risk_threshold = 70
        self.medium_risk_threshold = 40

        # Pattern to find churn keywords in text
        self.churn_keyword_pattern = re.compile(
            r'(לעזוב|לבטל|מתחרים|יקר|גרוע|לסיים|להפסיק|לעבור|מחיר|תלונה|ביטול|עוזב|'
            r'לנתק|ניוד|גולן|הוט|סלקום|פרטנר|להחליף|לצאת|לנייד|לא מרוצה|שירות גרוע)',
            re.UNICODE
        )

        logger.info("Evaluation service initialized")
        logger.info(f"Oracle host: {self.oracle_config['host']}")

    def get_connection(self):
        """Get Oracle database connection."""
        if not ORACLE_AVAILABLE:
            raise RuntimeError("oracledb not available")

        dsn = oracledb.makedsn(
            self.oracle_config['host'],
            self.oracle_config['port'],
            service_name=self.oracle_config['service_name']
        )
        return oracledb.connect(
            user=self.oracle_config['user'],
            password=self.oracle_config['password'],
            dsn=dsn
        )

    def execute_query(self, query: str, params: Dict = None) -> List[Dict]:
        """Execute query and return results as list of dicts."""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(query, params or {})
            columns = [col[0].lower() for col in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.error(f"Query error: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def run_weekly_evaluation(self) -> Dict[str, Any]:
        """
        Main entry point - run the weekly evaluation.

        Returns:
            Dict with evaluation results and recommendations
        """
        logger.info("=" * 60)
        logger.info("STARTING WEEKLY ML EVALUATION")
        logger.info(f"Timestamp: {datetime.now().isoformat()}")
        logger.info("=" * 60)

        results = {
            'timestamp': datetime.now().isoformat(),
            'metrics': {},
            'recommendations': [],
            'errors': []
        }

        try:
            # 1. Collect churned customers
            churned_data = self.collect_churned_customers(days=30)
            logger.info(f"Found {len(churned_data)} churned customers in last 30 days")
            results['metrics']['total_churned'] = len(churned_data)

            if not churned_data:
                logger.warning("No churned customers found - evaluation cannot proceed")
                results['errors'].append("No churned customers found in last 30 days")
                return results

            # 2. Evaluate churn predictions
            churn_metrics = self.evaluate_churn_predictions(churned_data)
            results['metrics']['churn'] = churn_metrics
            logger.info(f"Churn recall: {churn_metrics.get('recall', 0):.1%}")
            logger.info(f"Pipeline coverage: {churn_metrics.get('coverage', 0):.1%}")

            # 3. Analyze patterns in missed churners
            missed_churners = self.get_missed_churners(churned_data)
            patterns = self.analyze_patterns(missed_churners)
            results['patterns'] = patterns

            # 4. Generate recommendations (NOT auto-applied!)
            recommendations = self.generate_recommendations(churn_metrics, patterns)
            results['recommendations'] = recommendations

            # 5. Store recommendations for human review
            if recommendations:
                self.store_recommendations(recommendations)
                logger.info(f"Stored {len(recommendations)} recommendations for review")
            else:
                logger.info("No recommendations generated - system performing well")

            # 6. Analyze classification feedback (if any)
            feedback_analysis = self.analyze_classification_feedback()
            if feedback_analysis:
                self.store_recommendations(feedback_analysis)
                results['recommendations'].extend(feedback_analysis)

            # 7. Store evaluation history
            self.store_evaluation_history(results['metrics'])

            logger.info("=" * 60)
            logger.info("EVALUATION COMPLETE - Awaiting human approval in dashboard")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Evaluation error: {e}")
            results['errors'].append(str(e))

        return results

    def collect_churned_customers(self, days: int = 30) -> List[Dict]:
        """
        Find customers who churned and their conversation data.

        Uses MAX(CHURN_SCORE) per subscriber to check if we EVER flagged them
        across all their calls before churning.

        Args:
            days: Number of days to look back for churned customers

        Returns:
            List of churned customer records with their max churn score
        """
        query = """
            SELECT
                s.SUBSCRIBER_NO,
                s.STATUS as churn_status,
                s.STATUS_DATE as churn_date,
                MAX(cs.CHURN_SCORE) as max_churn_score,
                COUNT(DISTINCT v.CALL_ID) as call_count,
                LISTAGG(DISTINCT v.CALL_ID, ',') WITHIN GROUP (ORDER BY v.CALL_TIME DESC) as call_ids
            FROM SUBSCRIBER s
            JOIN VERINT_TEXT_ANALYSIS v ON s.SUBSCRIBER_NO = v.SUBSCRIBER_NO
            LEFT JOIN CONVERSATION_SUMMARY cs ON TO_CHAR(v.CALL_ID) = cs.SOURCE_ID
            WHERE s.STATUS IN ('CHURNED', 'PORTED', 'CANCELLED', 'DEACTIVATED')
            AND s.STATUS_DATE > SYSDATE - :days
            AND v.CALL_TIME < s.STATUS_DATE
            GROUP BY s.SUBSCRIBER_NO, s.STATUS, s.STATUS_DATE
        """

        return self.execute_query(query, {'days': days})

    def get_missed_churners(self, churned_data: List[Dict]) -> List[Dict]:
        """
        Get churned customers we failed to predict (score < threshold or NULL).

        Args:
            churned_data: List of churned customer records

        Returns:
            List of missed churner records with their call text
        """
        missed = []

        for customer in churned_data:
            score = customer.get('max_churn_score')
            # Missed if score is NULL or below medium risk threshold
            if score is None or score < self.medium_risk_threshold:
                # Get their last call transcript for pattern analysis
                call_ids = customer.get('call_ids', '')
                if call_ids:
                    first_call_id = call_ids.split(',')[0]  # Most recent call
                    transcript = self.get_call_transcript(first_call_id)
                    customer['conversation_text'] = transcript
                missed.append(customer)

        return missed

    def get_call_transcript(self, call_id: str) -> str:
        """Get transcript text for a specific call."""
        query = """
            SELECT DBMS_LOB.SUBSTR(TEXT, 4000, 1) as text
            FROM VERINT_TEXT_ANALYSIS
            WHERE CALL_ID = :call_id
            ORDER BY CALL_TIME
        """
        results = self.execute_query(query, {'call_id': call_id})
        return ' '.join(r.get('text', '') for r in results if r.get('text'))

    def evaluate_churn_predictions(self, churned_data: List[Dict]) -> Dict:
        """
        Calculate how well we predicted churn.

        Args:
            churned_data: List of churned customer records

        Returns:
            Dict with recall, coverage, and other metrics
        """
        if not churned_data:
            return {'recall': 0, 'coverage': 0, 'samples': 0}

        # Split into customers with and without scores
        with_score = [c for c in churned_data if c.get('max_churn_score') is not None]
        without_score = [c for c in churned_data if c.get('max_churn_score') is None]

        # Calculate metrics only on customers we processed
        if with_score:
            # Customers we flagged as high risk (>= 70)
            high_risk = sum(1 for c in with_score if c['max_churn_score'] >= self.high_risk_threshold)
            # Customers we flagged as medium+ risk (>= 40)
            medium_plus_risk = sum(1 for c in with_score if c['max_churn_score'] >= self.medium_risk_threshold)

            recall_high = high_risk / len(with_score)
            recall_medium = medium_plus_risk / len(with_score)
            avg_score = sum(c['max_churn_score'] for c in with_score) / len(with_score)
        else:
            recall_high = 0
            recall_medium = 0
            avg_score = 0

        # Coverage: what % of churners had processed calls?
        coverage = len(with_score) / len(churned_data) if churned_data else 0

        return {
            'total_churned': len(churned_data),
            'with_score': len(with_score),
            'without_score': len(without_score),
            'high_risk_caught': high_risk if with_score else 0,
            'medium_plus_caught': medium_plus_risk if with_score else 0,
            'recall_high': recall_high,          # Using 70+ threshold
            'recall_medium': recall_medium,      # Using 40+ threshold
            'recall': recall_medium,              # Primary recall metric
            'coverage': coverage,
            'avg_churn_score': avg_score,
            'samples': len(churned_data)
        }

    def analyze_patterns(self, missed_churners: List[Dict]) -> Dict:
        """
        Find keywords/patterns in conversations of churners we missed.

        Args:
            missed_churners: Customers who churned but we didn't flag

        Returns:
            Dict with discovered patterns and keyword counts
        """
        if not missed_churners:
            return {'keywords': [], 'keyword_counts': {}, 'sample_phrases': []}

        keyword_counts = Counter()
        sample_phrases = []

        for customer in missed_churners:
            text = customer.get('conversation_text', '')
            if not text:
                continue

            # Find all churn-related keywords
            matches = self.churn_keyword_pattern.findall(text)
            keyword_counts.update(matches)

            # Extract sample phrases for context
            if matches:
                # Get a sentence containing the keyword
                sentences = text.split('.')
                for sentence in sentences:
                    for match in matches[:2]:  # First 2 matches
                        if match in sentence and len(sentence) < 200:
                            sample_phrases.append(sentence.strip())
                            break

        # Keywords appearing in >10% of missed churner calls are significant
        min_occurrences = max(1, len(missed_churners) * 0.1)
        significant_keywords = [
            kw for kw, count in keyword_counts.items()
            if count >= min_occurrences
        ]

        return {
            'keywords': significant_keywords,
            'keyword_counts': dict(keyword_counts.most_common(20)),
            'sample_phrases': sample_phrases[:10],  # Top 10 sample phrases
            'missed_count': len(missed_churners)
        }

    def generate_recommendations(self, metrics: Dict, patterns: Dict) -> List[Dict]:
        """
        Generate recommendations for human review.

        Args:
            metrics: Churn prediction metrics
            patterns: Pattern analysis from missed churners

        Returns:
            List of recommendation dicts
        """
        recommendations = []

        # Recommend threshold change if recall too low
        recall = metrics.get('recall', 1)
        if recall < 0.5:
            recommendations.append({
                'type': 'churn_threshold',
                'current_value': self.high_risk_threshold,
                'recommended_value': self.medium_risk_threshold,  # Lower to 40
                'reason': f"Churn recall is only {recall:.1%}. Lowering the alert threshold will catch more churners.",
                'impact': 'May increase false positives but will catch more actual churners',
                'metrics': {
                    'current_recall': recall,
                    'missed_churners': metrics.get('with_score', 0) - metrics.get('medium_plus_caught', 0)
                }
            })

        # Recommend new keywords if patterns found
        if patterns.get('keywords'):
            # Filter to keywords that aren't already in our list
            # (In practice, you'd check against current config)
            new_keywords = patterns['keywords']

            if new_keywords:
                recommendations.append({
                    'type': 'churn_keywords',
                    'keywords': new_keywords,
                    'reason': f"Found {len(new_keywords)} keywords appearing frequently in conversations of churners we missed",
                    'keyword_counts': patterns.get('keyword_counts', {}),
                    'sample_phrases': patterns.get('sample_phrases', []),
                    'impact': f"Adding these keywords may help catch {patterns.get('missed_count', 0)} similar churners"
                })

        # Check for low coverage (pipeline not processing enough calls)
        coverage = metrics.get('coverage', 1)
        if coverage < 0.8:
            recommendations.append({
                'type': 'pipeline_coverage',
                'current_coverage': coverage,
                'reason': f"Only {coverage:.1%} of churner calls were processed by ML. {metrics.get('without_score', 0)} customers had no churn score.",
                'impact': 'Investigate why some calls are not being processed by the ML service'
            })

        return recommendations

    def store_recommendations(self, recommendations: List[Dict]):
        """
        Store recommendations in Oracle for dashboard review.

        Args:
            recommendations: List of recommendation dicts to store
        """
        if not ORACLE_AVAILABLE:
            logger.warning("Oracle not available - skipping recommendation storage")
            return

        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            for rec in recommendations:
                cursor.execute("""
                    INSERT INTO ML_CONFIG_RECOMMENDATIONS (
                        REC_ID, REC_TYPE, REC_DETAILS, STATUS, CREATED_AT
                    ) VALUES (
                        SYS_GUID(), :rec_type, :details, 'PENDING', SYSTIMESTAMP
                    )
                """, {
                    'rec_type': rec.get('type'),
                    'details': json.dumps(rec, ensure_ascii=False, default=str)
                })

            conn.commit()
            logger.info(f"Stored {len(recommendations)} recommendations for review")

        except Exception as e:
            logger.error(f"Error storing recommendations: {e}")
        finally:
            if conn:
                conn.close()

    def store_evaluation_history(self, metrics: Dict):
        """
        Store evaluation results for historical tracking.

        Args:
            metrics: Evaluation metrics dict
        """
        if not ORACLE_AVAILABLE:
            logger.warning("Oracle not available - skipping history storage")
            return

        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            churn_metrics = metrics.get('churn', {})

            cursor.execute("""
                INSERT INTO ML_EVALUATION_HISTORY (
                    EVAL_ID, EVAL_DATE, CHURNED_COUNT, WITH_SCORE_COUNT,
                    RECALL_RATE, COVERAGE_RATE, AVG_CHURN_SCORE,
                    RECOMMENDATIONS_GENERATED, NOTES
                ) VALUES (
                    SYS_GUID(), SYSTIMESTAMP, :churned, :with_score,
                    :recall, :coverage, :avg_score,
                    :recs_count, :notes
                )
            """, {
                'churned': churn_metrics.get('total_churned', 0),
                'with_score': churn_metrics.get('with_score', 0),
                'recall': churn_metrics.get('recall', 0),
                'coverage': churn_metrics.get('coverage', 0),
                'avg_score': churn_metrics.get('avg_churn_score', 0),
                'recs_count': len(metrics.get('recommendations', [])),
                'notes': json.dumps(metrics, ensure_ascii=False, default=str)
            })

            conn.commit()
            logger.info("Evaluation history stored successfully")

        except Exception as e:
            logger.error(f"Error storing evaluation history: {e}")
        finally:
            if conn:
                conn.close()

    def analyze_classification_feedback(self) -> List[Dict]:
        """
        Analyze human feedback on classifications to find patterns.

        Returns:
            List of recommendations based on feedback analysis
        """
        if not ORACLE_AVAILABLE:
            return []

        try:
            # Find common misclassifications (3+ occurrences)
            results = self.execute_query("""
                SELECT
                    ML_CATEGORY as predicted,
                    CORRECT_CATEGORY as actual,
                    COUNT(*) as error_count
                FROM ML_CLASSIFICATION_FEEDBACK
                WHERE IS_CORRECT = 0
                AND CREATED_AT > SYSDATE - 30
                GROUP BY ML_CATEGORY, CORRECT_CATEGORY
                HAVING COUNT(*) >= 3
                ORDER BY error_count DESC
            """)

            if not results:
                logger.info("No significant classification errors found in feedback")
                return []

            misclassifications = [
                {
                    'predicted': r['predicted'],
                    'actual': r['actual'],
                    'count': r['error_count']
                }
                for r in results
            ]

            return [{
                'type': 'classification_fix',
                'misclassifications': misclassifications,
                'reason': f"Human reviewers corrected these classifications {sum(m['count'] for m in misclassifications)} times",
                'impact': 'Consider adding keywords to differentiate these categories'
            }]

        except Exception as e:
            logger.error(f"Error analyzing classification feedback: {e}")
            return []


def main():
    """Main entry point for the evaluation service."""
    service = EvaluationService()
    results = service.run_weekly_evaluation()

    # Print summary
    print("\n" + "=" * 60)
    print("EVALUATION SUMMARY")
    print("=" * 60)
    print(f"Timestamp: {results['timestamp']}")
    print(f"Total churned customers: {results['metrics'].get('total_churned', 0)}")

    churn_metrics = results['metrics'].get('churn', {})
    print(f"Recall (40+ threshold): {churn_metrics.get('recall', 0):.1%}")
    print(f"Coverage: {churn_metrics.get('coverage', 0):.1%}")

    print(f"\nRecommendations generated: {len(results['recommendations'])}")
    for rec in results['recommendations']:
        print(f"  - {rec['type']}: {rec['reason'][:80]}...")

    if results['errors']:
        print(f"\nErrors: {results['errors']}")

    print("=" * 60)

    return results


if __name__ == '__main__':
    main()
