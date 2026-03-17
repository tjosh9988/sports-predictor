import 'package:flutter/material.dart';

// Remaining utility/placeholder screens
class SportScreen extends StatelessWidget { 
  final String slug; 
  const SportScreen({super.key, required this.slug}); 
  @override 
  Widget build(BuildContext context) => Scaffold(
    appBar: AppBar(title: Text(slug.toUpperCase())),
    body: Center(child: Text('Sport: $slug Content')),
  ); 
}
