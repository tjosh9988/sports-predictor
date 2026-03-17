class PerformanceModel {
  final int totalPredictions;
  final int won;
  final int lost;
  final int pending;
  final double winRate;
  final Map<String, dynamic> byType;
  final List<ModelAccuracyRecord> models;

  PerformanceModel({
    required this.totalPredictions,
    required this.won,
    required this.lost,
    required this.pending,
    required this.winRate,
    required this.byType,
    required this.models,
  });

  factory PerformanceModel.fromJson(Map<String, dynamic> json) {
    return PerformanceModel(
      totalPredictions: json['total_predictions'] ?? 0,
      won: json['won'] ?? 0,
      lost: json['lost'] ?? 0,
      pending: json['pending'] ?? 0,
      winRate: (json['win_rate'] as num? ?? 0.0).toDouble(),
      byType: Map<String, dynamic>.from(json['by_type'] ?? {}),
      models: (json['models'] as List? ?? [])
          .map((m) => ModelAccuracyRecord.fromJson(m as Map<String, dynamic>))
          .toList(),
    );
  }

  factory PerformanceModel.empty() {
    return PerformanceModel(
      totalPredictions: 0,
      won: 0,
      lost: 0,
      pending: 0,
      winRate: 0.0,
      byType: {},
      models: [],
    );
  }
}

class ModelAccuracyRecord {
  final String modelName;
  final double accuracy;
  final int totalBets;

  ModelAccuracyRecord({
    required this.modelName,
    required this.accuracy,
    required this.totalBets,
  });

  factory ModelAccuracyRecord.fromJson(Map<String, dynamic> json) {
    return ModelAccuracyRecord(
      modelName: json['model_name'] ?? 'Unknown',
      accuracy: (json['accuracy'] as num? ?? 0.0).toDouble(),
      totalBets: json['bets'] ?? 0,
    );
  }
}
