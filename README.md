https://claude.ai/share/0a8e2265-771d-400f-92a9-cf2a8f92a7fc



https://claude.ai/public/artifacts/96fdb415-cf3b-4a97-8383-fdea6a2d169f


https://claude.ai/public/artifacts/35f8aec9-7ee8-4948-8212-e2c0c9bfe0a5

https://claude.ai/public/artifacts/79e2dd56-ff64-44e8-a912-205576b80fb9


https://claude.ai/public/artifacts/f7b10b3f-3a36-43b1-96a9-bc571ccb22c8


# Dynamic Form — Librairie Nx pour Angular 17

## Contraintes respectées

| Contrainte | Status |
|---|---|
| Angular 17 uniquement | ✅ Pas de `@let`, pas de Signal Forms, pas de signal `input()` |
| Reactive Forms | ✅ `FormGroup`, `FormControl`, `FormArray` |
| Standalone component | ✅ Importable sans NgModule |
| Signals pour le DependencyEngine | ✅ `signal()`, `computed()`, `effect()` (stables en v17) |
| Nouveau control flow | ✅ `@if`, `@for` (stable en Angular 17) |
| Buildable comme lib Nx | ✅ Aucune dépendance tierce |
| Importable dans un projet Angular 17 externe | ✅ Sélecteur préfixé `lib-`, exports propres |
| OnPush | ✅ `ChangeDetectionStrategy.OnPush` |

## Ce qui n'est PAS utilisé (car Angular 18+/19+/20+/21+)

- `@let` dans les templates → Angular 18.1+
- `signal()` inputs (`input()`, `output()`) → stable à partir d'Angular 17.1-17.2 en dev preview, mais pas recommandé pour une lib à distribuer
- `linkedSignal()` → Angular 19+
- `resource()` → Angular 19+
- Signal Forms (`form()`, `FormField`, `@angular/forms/signals`) → **Angular 21 uniquement**
- Zoneless change detection → stable à partir d'Angular 20.2

---

## Structure Nx recommandée

```
libs/
  ui-dynamic-form/
    src/
      index.ts                          ← public-api (barrel)
      lib/
        dynamic-form/
          models.ts                     ← Interfaces & types
          dependency-engine.ts          ← Service DependencyEngine (signals)
          form-builder.service.ts       ← Service construction du FormGroup
          dynamic-form.component.ts     ← Composant principal
          dynamic-field-template.directive.ts  ← Directive pour templates custom
    project.json                        ← Config Nx
    tsconfig.lib.json
    ng-package.json                     ← Config ng-packagr
```

### Génération de la lib Nx

```bash
nx generate @nx/angular:library ui-dynamic-form \
  --directory=libs/ui-dynamic-form \
  --standalone \
  --prefix=lib \
  --buildable \
  --publishable \
  --importPath=@myorg/ui-dynamic-form
```

### public-api (`index.ts`)

```typescript
// libs/ui-dynamic-form/src/index.ts

// Models
export type {
  ControlType,
  DependencyOperator,
  ConditionLogic,
  SelectOption,
  DependencyCondition,
  DependencyEffect,
  DependencyRule,
  ValidatorConfig,
  LayoutConfig,
  FieldRuntimeState,
  FieldConfig,
  FormConfig,
} from './lib/dynamic-form/models';

// Services
export { DependencyEngine } from './lib/dynamic-form/dependency-engine';
export { DynamicFormBuilderService } from './lib/dynamic-form/form-builder.service';

// Component & Directive
export { DynamicFormComponent } from './lib/dynamic-form/dynamic-form.component';
export { DynamicFieldTemplateDirective } from './lib/dynamic-form/dynamic-form.component';
```

---

## Utilisation dans le projet Angular 17 consommateur

### 1. Installer la librairie

```bash
# Si publiée sur npm
npm install @myorg/ui-dynamic-form

# Si monorepo Nx (path mapping dans tsconfig.base.json)
# Déjà configuré automatiquement par Nx
```

### 2. Importer dans un composant standalone

```typescript
import { Component } from '@angular/core';
import { DynamicFormComponent } from '@myorg/ui-dynamic-form';
import type { FormConfig } from '@myorg/ui-dynamic-form';

@Component({
  selector: 'app-my-page',
  standalone: true,
  imports: [DynamicFormComponent],  // ← Standalone, pas besoin de NgModule
  template: `
    <lib-dynamic-form
      [config]="formConfig"
      (formSubmit)="onSubmit($event)"
      (formValueChange)="onChange($event)"
    />
  `,
})
export class MyPageComponent {
  formConfig: FormConfig = {
    title: 'Mon formulaire',
    layout: { columns: 2 },
    submitLabel: 'Envoyer',
    disableSubmitIfInvalid: true,
    fields: [
      {
        key: 'name',
        type: 'text',
        label: 'Nom',
        validators: [{ type: 'required', message: 'Le nom est requis' }],
      },
      {
        key: 'email',
        type: 'email',
        label: 'Email',
        validators: [
          { type: 'required', message: 'Requis' },
          { type: 'email', message: 'Email invalide' },
        ],
      },
    ],
  };

  onSubmit(values: Record<string, any>): void {
    console.log('Submitted:', values);
  }

  onChange(values: Record<string, any>): void {
    console.log('Changed:', values);
  }
}
```

### 3. Ou importer dans un NgModule (projet legacy)

```typescript
import { NgModule } from '@angular/core';
import { DynamicFormComponent } from '@myorg/ui-dynamic-form';

@NgModule({
  imports: [DynamicFormComponent],  // Standalone → importable dans NgModule
  // ...
})
export class MyFeatureModule {}
```

### 4. Template personnalisé

```typescript
import { DynamicFormComponent, DynamicFieldTemplateDirective } from '@myorg/ui-dynamic-form';

@Component({
  imports: [DynamicFormComponent, DynamicFieldTemplateDirective],
  template: `
    <lib-dynamic-form [config]="config">
      <ng-template dynamicFieldTemplate="colorPicker"
                   let-field let-control="control">
        <input type="color"
               [value]="control.value"
               (input)="control.setValue($event.target.value)" />
      </ng-template>
    </lib-dynamic-form>
  `,
})
```

---

## Peer dependencies (à mettre dans le `package.json` de la lib)

```json
{
  "peerDependencies": {
    "@angular/core": "^17.0.0",
    "@angular/common": "^17.0.0",
    "@angular/forms": "^17.0.0",
    "rxjs": "^7.0.0"
  }
}
```

Cela garantit la compatibilité Angular 17 tout en permettant les versions supérieures.

---

## Migration future vers Angular 21 Signal Forms

Quand le projet consommateur sera prêt pour Angular 21, la migration sera incrémentale :

1. **Le JSON de config ne change pas** — même interface `FormConfig` et `FieldConfig`
2. **Le DependencyEngine ne change pas** — il est déjà basé sur signals
3. **Ce qui change** : le composant interne remplace `FormGroup`/`FormControl` par `form(signal)` + `[formField]`
4. **Pour le consommateur** : aucun changement d'API, seule la dépendance Angular passe à 21

Stratégie :
- Créer une v2 de la lib qui utilise Signal Forms sous le capot
- Le public-api reste identique
- Les `peerDependencies` passent à `^21.0.0`

---

## Fichiers fournis

| Fichier | Description |
|---------|-------------|
| `dynamic-form.component.ts` | **Fichier unique** contenant : models, engine, builder service, composant, directive. Pour la prod, découper selon la structure Nx ci-dessus. |
| `README.md` | Cette documentation |
