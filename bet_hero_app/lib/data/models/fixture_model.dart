class FixtureModel {
  final String id;
  final String sport;
  final String league;
  final String homeTeam;
  final String awayTeam;
  final DateTime matchDate;
  final String status;
  final int? predictionId;
  final double? homeOdds;
  final double? drawOdds;
  final double? awayOdds;
  final int? homeScore;
  final int? awayScore;
  final String? venue;
  final String? round;

  FixtureModel({
    required this.id,
    required this.sport,
    required this.league,
    required this.homeTeam,
    required this.awayTeam,
    required this.matchDate,
    required this.status,
    this.predictionId,
    this.homeOdds,
    this.drawOdds,
    this.awayOdds,
    this.homeScore,
    this.awayScore,
    this.venue,
    this.round,
  });

  factory FixtureModel.fromJson(Map<String, dynamic> json) {
    return FixtureModel(
      id: json['id']?.toString() ?? '',
      sport: json['sport'] ?? '',
      league: json['league'] ?? '',
      homeTeam: json['home_team'] ?? '',
      awayTeam: json['away_team'] ?? '',
      matchDate: json['match_date'] != null 
          ? DateTime.parse(json['match_date']) 
          : DateTime.now(),
      status: json['status'] ?? '',
      predictionId: json['prediction_id'],
      homeOdds: (json['home_odds'] as num?)?.toDouble(),
      drawOdds: (json['draw_odds'] as num?)?.toDouble(),
      awayOdds: (json['away_odds'] as num?)?.toDouble(),
      homeScore: json['home_score'],
      awayScore: json['away_score'],
      venue: json['venue'] ?? '',
      round: json['round'] ?? '',
    );
  }
}
