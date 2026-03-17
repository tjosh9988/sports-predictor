class AccumulatorLegModel {
  final String id;
  final String accumulatorId;
  final String? matchId;
  final String sport;
  final String league;
  final String homeTeam;
  final String awayTeam;
  final String market;
  final String predictedOutcome;
  final double odds;
  final double confidence;
  final double edge;
  final String aiReasoning;
  final String status;
  final String? actualOutcome;
  final int legOrder;

  AccumulatorLegModel({
    required this.id,
    required this.accumulatorId,
    this.matchId,
    required this.sport,
    required this.league,
    required this.homeTeam,
    required this.awayTeam,
    required this.market,
    required this.predictedOutcome,
    required this.odds,
    required this.confidence,
    required this.edge,
    required this.aiReasoning,
    required this.status,
    this.actualOutcome,
    required this.legOrder,
  });

  factory AccumulatorLegModel.fromJson(Map<String, dynamic> json) {
    return AccumulatorLegModel(
      id: json['id']?.toString() ?? '',
      accumulatorId: json['accumulator_id']?.toString() ?? '',
      matchId: json['match_id']?.toString(),
      homeTeam: json['home_team']?.toString() ?? 'Unknown',
      awayTeam: json['away_team']?.toString() ?? 'Unknown',
      league: json['league']?.toString() ?? '',
      sport: json['sport']?.toString() ?? '',
      market: json['market']?.toString() ?? 'Match Result',
      predictedOutcome: json['predicted_outcome']?.toString() ?? '',
      odds: (json['odds'] as num?)?.toDouble() ?? 0.0,
      confidence: (json['confidence'] as num?)?.toDouble() ?? 0.0,
      edge: (json['edge'] as num?)?.toDouble() ?? 0.0,
      aiReasoning: json['ai_reasoning']?.toString() ?? '',
      status: json['status']?.toString() ?? 'PENDING',
      legOrder: (json['leg_order'] as num?)?.toInt() ?? 0,
    );
  }
}
