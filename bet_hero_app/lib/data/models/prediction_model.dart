class PredictionModel {
  final int id;
  final int matchId;
  final String market;
  final String predictedOutcome;
  final double modelProbability;
  final double impliedProbability;
  final double edge;
  final double odds;
  final double confidenceScore;
  final String status;
  final String? actualOutcome;
  final String? result; // Alias for actualOutcome/status in UI
  final String? aiReasoning;
  final DateTime matchDate;
  final DateTime createdAt;

  PredictionModel({
    required this.id,
    required this.matchId,
    required this.market,
    required this.predictedOutcome,
    required this.modelProbability,
    required this.impliedProbability,
    required this.edge,
    required this.odds,
    required this.confidenceScore,
    required this.status,
    this.actualOutcome,
    this.result,
    this.aiReasoning,
    required this.matchDate,
    required this.createdAt,
  });

  factory PredictionModel.fromJson(Map<String, dynamic> json) {
    return PredictionModel(
      id: json['id'],
      matchId: json['match_id'],
      market: json['market'] ?? '',
      predictedOutcome: json['predicted_outcome'] ?? '',
      modelProbability: (json['model_probability'] as num? ?? 0.0).toDouble(),
      impliedProbability: (json['implied_probability'] as num? ?? 0.0).toDouble(),
      edge: (json['edge'] as num? ?? 0.0).toDouble(),
      odds: (json['odds'] as num? ?? 0.0).toDouble(),
      confidenceScore: (json['confidence_score'] as num? ?? 0.0).toDouble(),
      status: json['status'] ?? 'pending',
      actualOutcome: json['actual_outcome'],
      result: json['result'] ?? json['actual_outcome'],
      aiReasoning: json['ai_reasoning'],
      matchDate: json['match_date'] != null 
          ? DateTime.parse(json['match_date']) 
          : DateTime.now(), // Fallback to now if missing
      createdAt: DateTime.parse(json['created_at']),
    );
  }
}
