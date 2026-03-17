class PerformanceModel {
  final String sport;
  final int totalPredictions;
  final int correctPredictions;
  final double winRate;
  final double roi;
  final int longestWinStreak;
  final int longestLossStreak;
  final Map<String, double> byAccaType;

  PerformanceModel({
    required this.sport,
    required this.totalPredictions,
    required this.correctPredictions,
    required this.winRate,
    required this.roi,
    required this.longestWinStreak,
    required this.longestLossStreak,
    required this.byAccaType,
  });

  factory PerformanceModel.fromJson(Map<String, dynamic> json) {
    return PerformanceModel(
      sport: json['sport'] ?? 'Overall',
      totalPredictions: json['total_predictions'] ?? 0,
      correctPredictions: json['correct_predictions'] ?? 0,
      winRate: (json['win_rate'] as num? ?? 0.0).toDouble(),
      roi: (json['roi'] as num? ?? 0.0).toDouble(),
      longestWinStreak: json['longest_win_streak'] ?? 0,
      longestLossStreak: json['longest_loss_streak'] ?? 0,
      byAccaType: Map<String, double>.from(json['by_acca_type'] ?? {}),
    );
  }

  factory PerformanceModel.empty() {
    return PerformanceModel(
      sport: 'Overall',
      totalPredictions: 0,
      correctPredictions: 0,
      winRate: 0.0,
      roi: 0.0,
      longestWinStreak: 0,
      longestLossStreak: 0,
      byAccaType: {},
    );
  }
}
