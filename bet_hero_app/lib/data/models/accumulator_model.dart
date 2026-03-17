import 'accumulator_leg_model.dart';

enum AccaType {
  three('3odds'),
  five('5odds'),
  ten('10odds');

  final String value;
  const AccaType(this.value);

  static AccaType fromString(String value) {
    return AccaType.values.firstWhere(
      (e) => e.value == value,
      orElse: () => AccaType.ten,
    );
  }
}

enum AccaStatus {
  pending('pending'),
  won('won'),
  lost('lost');

  final String value;
  const AccaStatus(this.value);

  static AccaStatus fromString(String value) {
    return AccaStatus.values.firstWhere(
      (e) => e.value == value,
      orElse: () => AccaStatus.pending,
    );
  }
}

class AccumulatorModel {
  final String id;
  final AccaType type;
  final double totalOdds;
  final AccaStatus status;
  final String? aiReasoning;
  final double confidenceScore;
  final DateTime createdAt;
  final List<AccumulatorLegModel> legs;

  AccumulatorModel({
    required this.id,
    required this.type,
    required this.totalOdds,
    required this.status,
    this.aiReasoning,
    required this.confidenceScore,
    required this.createdAt,
    required this.legs,
  });

  factory AccumulatorModel.fromJson(Map<String, dynamic> json) {
    final legsList = json['legs'] as List<dynamic>? ?? [];
    print('Parsing accumulator: ${json['acca_type']} '
          'with ${legsList.length} legs');
    
    return AccumulatorModel(
      id: json['id']?.toString() ?? '',
      type: AccaType.fromString(json['acca_type'] ?? ''),
      totalOdds: (json['total_odds'] as num?)?.toDouble() ?? 0.0,
      status: AccaStatus.fromString(json['status'] ?? 'PENDING'),
      aiReasoning: json['ai_reasoning'] ?? '',
      confidenceScore: (json['confidence_score'] as num?)?.toDouble() ?? 0.0,
      createdAt: json['created_at'] != null 
          ? DateTime.parse(json['created_at']) 
          : DateTime.now(),
      legs: legsList
          .map((l) => AccumulatorLegModel.fromJson(l as Map<String, dynamic>))
          .toList(),
    );
  }
}

