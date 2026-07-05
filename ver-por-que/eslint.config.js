import js from '@eslint/js';

export default [
  js.configs.recommended,
  {
    languageOptions: {
      ecmaVersion: 2024,
      sourceType: 'module',
      globals: {
        // Browser globals
        window: 'readonly',
        document: 'readonly',
        console: 'readonly',
        performance: 'readonly',
        localStorage: 'readonly',
        setTimeout: 'readonly',
        clearTimeout: 'readonly',
        requestAnimationFrame: 'readonly',
        fetch: 'readonly',
        indexedDB: 'readonly',
        getComputedStyle: 'readonly',
        CustomEvent: 'readonly',

        // CommonJS export guards (module.exports checks in browser scripts)
        module: 'readonly',

        // App classes/helpers shared as script globals across files
        FileStructureAnalyzer: 'readonly',
        InfoPanelManager: 'readonly',
        ParquetConstants: 'readonly',
        ParquetSegment: 'readonly',
        ParquetTypeResolver: 'readonly',
        SegmentHierarchyBuilder: 'readonly',
        SegmentLayoutCalculator: 'readonly',
        SvgByteVisualizer: 'readonly',
        VisualizationConfig: 'readonly',
        formatBytes: 'readonly',
        formatNumber: 'readonly',
        formatOffset: 'readonly',

        // Node.js globals (for tests)
        process: 'readonly',
        Buffer: 'readonly',
        global: 'readonly',

        // Jest globals
        describe: 'readonly',
        test: 'readonly',
        expect: 'readonly',
        beforeEach: 'readonly',
        afterEach: 'readonly',
        beforeAll: 'readonly',
        afterAll: 'readonly',
        jest: 'readonly',

        // Fast-check globals
        fc: 'readonly',
      },
    },
    rules: {
      // Code quality
      'no-unused-vars': [
        'error',
        {
          argsIgnorePattern: '^_',
          varsIgnorePattern: '^_',
          caughtErrors: 'none',
        },
      ],
      'no-console': 'off', // Allow console in this visualization project
      'prefer-const': 'error',
      'no-var': 'error',

      // Style (handled by Prettier, but some logical rules)
      eqeqeq: ['error', 'always'],
      curly: ['error', 'all'],

      // Potential issues
      'no-duplicate-imports': 'error',
      'no-unreachable': 'error',
      'no-constant-condition': ['error', { checkLoops: false }],

      // Best practices for this mathematical code
      'no-magic-numbers': 'off', // Mathematical constants are OK
      complexity: ['warn', 15], // Allow some complexity for algorithms
    },
    ignores: ['node_modules/**', 'dist/**', 'coverage/**', '*.config.js'],
  },
];
