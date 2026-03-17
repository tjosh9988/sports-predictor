class SportModel {
  final int id;
  final String name;
  final String slug;
  final String? iconPath;
  final bool isActive;

  SportModel({
    required this.id,
    required this.name,
    required this.slug,
    this.iconPath,
    required this.isActive,
  });

  factory SportModel.fromJson(Map<String, dynamic> json) {
    return SportModel(
      id: json['id'],
      name: json['name'] ?? '',
      slug: json['slug'] ?? '',
      iconPath: json['icon_path'],
      isActive: json['is_active'] ?? true,
    );
  }
}
