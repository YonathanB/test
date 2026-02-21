// ============================================================================
// DYNAMIC FORM COMPONENT — Angular 17 / Nx Library Compatible
// ============================================================================
//
// CONTRAINTES :
//   ✅ Angular 17 (pas de @let, pas de Signal Forms, pas de signal inputs)
//   ✅ Reactive Forms (FormGroup, FormControl, FormArray)
//   ✅ Standalone component (importable sans NgModule)
//   ✅ Signals (signal, computed, effect) pour le DependencyEngine
//   ✅ Nouveau control flow (@if, @for) — disponible Angular 17
//   ✅ Buildable en tant que librairie Nx
//   ✅ Importable dans un projet Angular 17 externe
//   ✅ OnPush change detection
//   ✅ Pas de dépendances tierces
//
// ARCHITECTURE :
//   FormConfig (JSON)
//     ├── DynamicFormComponent     → Orchestre rendu + réactivité
//     ├── DependencyEngine         → Évalue les dépendances via computed()
//     └── FormBuilderService       → Construit le FormGroup depuis la config
//
// EXPORTS depuis le public-api de la lib Nx :
//   export * from './lib/dynamic-form/dynamic-form.component';
//   export * from './lib/dynamic-form/models';
//   export * from './lib/dynamic-form/dependency-engine';
//   export * from './lib/dynamic-form/form-builder.service';
//
// ============================================================================


// ═══════════════════════════════════════════════════════════════════════════
// FICHIER 1 : models.ts — Interfaces & Types
// ═══════════════════════════════════════════════════════════════════════════

// --- Copier dans : libs/<your-lib>/src/lib/dynamic-form/models.ts ---

export type ControlType =
  | 'text'
  | 'number'
  | 'email'
  | 'password'
  | 'textarea'
  | 'select'
  | 'radio'
  | 'checkbox'
  | 'date'
  | 'time'
  | 'toggle'
  | 'slider'
  | 'file'
  | 'hidden'
  | 'group'
  | 'array'
  | 'custom';

export type DependencyOperator =
  | 'equals'
  | 'notEquals'
  | 'contains'
  | 'notContains'
  | 'greaterThan'
  | 'lessThan'
  | 'in'
  | 'notIn'
  | 'isEmpty'
  | 'isNotEmpty'
  | 'matches'
  | 'custom';

export type ConditionLogic = 'AND' | 'OR';

export interface SelectOption {
  label: string;
  value: any;
  disabled?: boolean;
  group?: string;
  icon?: string;
  description?: string;
}

export interface DependencyCondition {
  field: string;
  operator: DependencyOperator;
  value?: any;
  customFn?: (fieldValue: any, formValues: Record<string, any>) => boolean;
}

export interface DependencyEffect {
  visible?: boolean;
  disabled?: boolean;
  required?: boolean;
  readonly?: boolean;
  placeholder?: string;
  label?: string;
  options?: SelectOption[] | ((formValues: Record<string, any>) => SelectOption[]);
  optionsLoader?: (formValues: Record<string, any>) => Promise<SelectOption[]>;
  setValue?: any | ((formValues: Record<string, any>) => any);
  resetValue?: boolean;
  helpText?: string;
  cssClass?: string;
  min?: number | string;
  max?: number | string;
  pattern?: string;
  validators?: ValidatorConfig[];
}

export interface DependencyRule {
  id?: string;
  conditions: DependencyCondition[];
  logic?: ConditionLogic;
  effects: DependencyEffect;
  elseEffects?: DependencyEffect;
}

export interface ValidatorConfig {
  type: 'required' | 'min' | 'max' | 'minLength' | 'maxLength'
      | 'pattern' | 'email' | 'custom';
  value?: any;
  message: string;
  customFn?: (value: any, formValues: Record<string, any>) => boolean;
}

export interface LayoutConfig {
  colSpan?: number;
  wrapperClass?: string;
  order?: number;
  separator?: boolean;
  sectionTitle?: string;
  sectionDescription?: string;
}

export interface FieldRuntimeState {
  visible: boolean;
  disabled: boolean;
  required: boolean;
  readonly: boolean;
  placeholder: string;
  label: string;
  options: SelectOption[];
  helpText: string;
  cssClass: string;
  loading: boolean;
  min?: number | string;
  max?: number | string;
}

export interface FieldConfig {
  key: string;
  type: ControlType;
  label?: string;
  placeholder?: string;
  defaultValue?: any;
  tooltip?: string;
  helpText?: string;
  prefix?: string;
  suffix?: string;
  disabled?: boolean;
  hidden?: boolean;
  readonly?: boolean;

  options?: SelectOption[];
  optionsLoader?: (formValues: Record<string, any>) => Promise<SelectOption[]>;

  validators?: ValidatorConfig[];
  dependencies?: DependencyRule[];
  layout?: LayoutConfig;

  children?: FieldConfig[];
  arrayConfig?: {
    itemFields: FieldConfig[];
    minItems?: number;
    maxItems?: number;
    addLabel?: string;
    removeLabel?: string;
  };

  customTemplateRef?: string;
  cssClass?: string;
  attributes?: Record<string, string>;

  onChange?: (value: any, formValues: Record<string, any>) => void;
  onFocus?: (formValues: Record<string, any>) => void;
  onBlur?: (value: any, formValues: Record<string, any>) => void;
}

export interface FormConfig {
  id?: string;
  title?: string;
  description?: string;
  fields: FieldConfig[];
  layout?: {
    columns?: number;
    labelPosition?: 'top' | 'left' | 'floating';
    size?: 'sm' | 'md' | 'lg';
  };
  submitLabel?: string;
  showReset?: boolean;
  resetLabel?: string;
  disableSubmitIfInvalid?: boolean;
  cssClass?: string;
}


// ═══════════════════════════════════════════════════════════════════════════
// FICHIER 2 : dependency-engine.ts — Moteur de dépendances (Signals)
// ═══════════════════════════════════════════════════════════════════════════

// --- Copier dans : libs/<your-lib>/src/lib/dynamic-form/dependency-engine.ts ---

import { Injectable, signal, computed, type Signal, type WritableSignal } from '@angular/core';
import type {
  FieldConfig, FieldRuntimeState, DependencyCondition,
  DependencyRule, DependencyEffect, SelectOption,
} from './models';
import type { FormGroup, AbstractControl } from '@angular/forms';
import { Validators, type ValidatorFn, type ValidationErrors } from '@angular/forms';

@Injectable()
export class DependencyEngine {

  // ── Évaluation des conditions ──

  evaluateCondition(condition: DependencyCondition, formValues: Record<string, any>): boolean {
    const fieldValue = this.getNestedValue(formValues, condition.field);

    switch (condition.operator) {
      case 'equals':        return fieldValue === condition.value;
      case 'notEquals':     return fieldValue !== condition.value;
      case 'contains':
        if (Array.isArray(fieldValue)) return fieldValue.includes(condition.value);
        if (typeof fieldValue === 'string') return fieldValue.includes(condition.value);
        return false;
      case 'notContains':
        if (Array.isArray(fieldValue)) return !fieldValue.includes(condition.value);
        if (typeof fieldValue === 'string') return !fieldValue.includes(condition.value);
        return true;
      case 'greaterThan':   return fieldValue > condition.value;
      case 'lessThan':      return fieldValue < condition.value;
      case 'in':            return Array.isArray(condition.value) && condition.value.includes(fieldValue);
      case 'notIn':         return Array.isArray(condition.value) && !condition.value.includes(fieldValue);
      case 'isEmpty':
        return fieldValue == null || fieldValue === '' ||
               (Array.isArray(fieldValue) && fieldValue.length === 0);
      case 'isNotEmpty':
        return fieldValue != null && fieldValue !== '' &&
               !(Array.isArray(fieldValue) && fieldValue.length === 0);
      case 'matches':
        return typeof fieldValue === 'string' && new RegExp(condition.value).test(fieldValue);
      case 'custom':
        return condition.customFn ? condition.customFn(fieldValue, formValues) : false;
      default: return false;
    }
  }

  evaluateRule(rule: DependencyRule, formValues: Record<string, any>): boolean {
    const logic = rule.logic || 'AND';
    return logic === 'AND'
      ? rule.conditions.every(c => this.evaluateCondition(c, formValues))
      : rule.conditions.some(c => this.evaluateCondition(c, formValues));
  }

  // ── Calcul de l'état runtime ──

  computeFieldState(field: FieldConfig, formValues: Record<string, any>): FieldRuntimeState {
    const state: FieldRuntimeState = {
      visible: !field.hidden,
      disabled: field.disabled || false,
      required: field.validators?.some(v => v.type === 'required') || false,
      readonly: field.readonly || false,
      placeholder: field.placeholder || '',
      label: field.label || '',
      options: field.options ? [...field.options] : [],
      helpText: field.helpText || '',
      cssClass: field.cssClass || '',
      loading: false,
      min: undefined,
      max: undefined,
    };

    if (field.dependencies) {
      for (const rule of field.dependencies) {
        const met = this.evaluateRule(rule, formValues);
        const effects = met ? rule.effects : rule.elseEffects;
        if (effects) {
          this.mergeEffects(state, effects, formValues);
        }
      }
    }

    return state;
  }

  private mergeEffects(
    state: FieldRuntimeState,
    effects: DependencyEffect,
    formValues: Record<string, any>
  ): void {
    if (effects.visible !== undefined) state.visible = effects.visible;
    if (effects.disabled !== undefined) state.disabled = effects.disabled;
    if (effects.required !== undefined) state.required = effects.required;
    if (effects.readonly !== undefined) state.readonly = effects.readonly;
    if (effects.placeholder !== undefined) state.placeholder = effects.placeholder;
    if (effects.label !== undefined) state.label = effects.label;
    if (effects.helpText !== undefined) state.helpText = effects.helpText;
    if (effects.cssClass !== undefined) state.cssClass = effects.cssClass;
    if (effects.min !== undefined) state.min = effects.min;
    if (effects.max !== undefined) state.max = effects.max;

    if (effects.options !== undefined) {
      state.options = typeof effects.options === 'function'
        ? effects.options(formValues)
        : effects.options;
    }
  }

  // ── Application des effets sur le FormGroup ──

  applyControlEffects(
    field: FieldConfig,
    state: FieldRuntimeState,
    formGroup: FormGroup,
    formValues: Record<string, any>,
    previousState?: FieldRuntimeState
  ): void {
    const control = this.getControl(formGroup, field.key);
    if (!control) return;

    // Disabled / Enabled
    if (!previousState || previousState.disabled !== state.disabled) {
      if (state.disabled && !control.disabled) {
        control.disable({ emitEvent: false });
      } else if (!state.disabled && control.disabled) {
        control.enable({ emitEvent: false });
      }
    }

    // Validators dynamiques
    if (!previousState || previousState.required !== state.required) {
      const validators = this.buildValidators(field, state);
      control.setValidators(validators);
      control.updateValueAndValidity({ emitEvent: false });
    }
  }

  applySideEffects(
    field: FieldConfig,
    formGroup: FormGroup,
    formValues: Record<string, any>
  ): void {
    if (!field.dependencies) return;

    for (const rule of field.dependencies) {
      const met = this.evaluateRule(rule, formValues);
      const effects = met ? rule.effects : rule.elseEffects;
      if (!effects) continue;

      const control = this.getControl(formGroup, field.key);
      if (!control) continue;

      // setValue
      if (effects.setValue !== undefined) {
        const newVal = typeof effects.setValue === 'function'
          ? effects.setValue(formValues)
          : effects.setValue;
        if (control.value !== newVal) {
          control.setValue(newVal, { emitEvent: false });
        }
      }

      // resetValue
      if (effects.resetValue) {
        const defaultVal = field.defaultValue ?? null;
        if (control.value !== defaultVal) {
          control.reset(defaultVal, { emitEvent: false });
        }
      }
    }
  }

  // ── Validators ──

  buildValidators(field: FieldConfig, state?: FieldRuntimeState): ValidatorFn[] {
    const validators: ValidatorFn[] = [];

    if (state?.required) {
      validators.push(Validators.required);
    }

    if (field.validators) {
      for (const v of field.validators) {
        switch (v.type) {
          case 'required':
            if (!state?.required) validators.push(Validators.required);
            break;
          case 'min':       validators.push(Validators.min(v.value)); break;
          case 'max':       validators.push(Validators.max(v.value)); break;
          case 'minLength': validators.push(Validators.minLength(v.value)); break;
          case 'maxLength': validators.push(Validators.maxLength(v.value)); break;
          case 'pattern':   validators.push(Validators.pattern(v.value)); break;
          case 'email':     validators.push(Validators.email); break;
          case 'custom':
            if (v.customFn) {
              const fn = v.customFn;
              const msg = v.message;
              validators.push((ctrl: AbstractControl): ValidationErrors | null => {
                const formValues = ctrl.parent ? (ctrl.parent as FormGroup).getRawValue() : {};
                return fn(ctrl.value, formValues) ? null : { custom: msg };
              });
            }
            break;
        }
      }
    }

    return validators;
  }

  getErrorMessage(field: FieldConfig, control: AbstractControl): string | null {
    if (!control.errors || !control.touched) return null;

    if (control.errors['required']) {
      return field.validators?.find(v => v.type === 'required')?.message
        || `${field.label || field.key} est requis`;
    }
    if (control.errors['min']) {
      return field.validators?.find(v => v.type === 'min')?.message
        || `Valeur minimum : ${control.errors['min'].min}`;
    }
    if (control.errors['max']) {
      return field.validators?.find(v => v.type === 'max')?.message
        || `Valeur maximum : ${control.errors['max'].max}`;
    }
    if (control.errors['minlength']) {
      return field.validators?.find(v => v.type === 'minLength')?.message
        || `Minimum ${control.errors['minlength'].requiredLength} caractères`;
    }
    if (control.errors['maxlength']) {
      return field.validators?.find(v => v.type === 'maxLength')?.message
        || `Maximum ${control.errors['maxlength'].requiredLength} caractères`;
    }
    if (control.errors['email']) {
      return field.validators?.find(v => v.type === 'email')?.message || 'Email invalide';
    }
    if (control.errors['pattern']) {
      return field.validators?.find(v => v.type === 'pattern')?.message || 'Format invalide';
    }
    if (control.errors['custom']) {
      return control.errors['custom'];
    }
    return 'Valeur invalide';
  }

  // ── Utilitaires ──

  getNestedValue(obj: any, path: string): any {
    return path.split('.').reduce((acc, key) => acc?.[key], obj);
  }

  private getControl(formGroup: FormGroup, key: string): AbstractControl | null {
    const parts = key.split('.');
    let control: AbstractControl | null = formGroup;
    for (const part of parts) {
      control = (control as FormGroup).get(part);
      if (!control) return null;
    }
    return control;
  }
}


// ═══════════════════════════════════════════════════════════════════════════
// FICHIER 3 : form-builder.service.ts
// ═══════════════════════════════════════════════════════════════════════════

// --- Copier dans : libs/<your-lib>/src/lib/dynamic-form/form-builder.service.ts ---

import { Injectable, inject } from '@angular/core';
import {
  FormBuilder, FormGroup, FormControl, FormArray,
  type AbstractControl,
} from '@angular/forms';
import type { FieldConfig, ControlType } from './models';
// import { DependencyEngine } from './dependency-engine';

// Note : DependencyEngine est injecté dans le composant parent,
// on le passe en paramètre ici pour éviter les problèmes de DI
// dans un contexte de librairie Nx.

@Injectable()
export class DynamicFormBuilderService {

  private fb = inject(FormBuilder);

  buildForm(fields: FieldConfig[], engine: DependencyEngine): FormGroup {
    const group: Record<string, AbstractControl> = {};

    for (const field of fields) {
      if (field.type === 'group' && field.children) {
        group[field.key] = this.buildForm(field.children, engine);
      } else if (field.type === 'array' && field.arrayConfig) {
        const initialItems = Array.isArray(field.defaultValue) ? field.defaultValue : [];
        const formArray = this.fb.array(
          initialItems.map(() => this.buildForm(field.arrayConfig!.itemFields, engine))
        );
        group[field.key] = formArray;
      } else {
        const validators = engine.buildValidators(field);
        const control = new FormControl(
          { value: field.defaultValue ?? this.getDefaultForType(field.type), disabled: field.disabled || false },
          validators
        );
        group[field.key] = control;
      }
    }

    return this.fb.group(group);
  }

  addArrayItem(formArray: FormArray, itemFields: FieldConfig[], engine: DependencyEngine): void {
    formArray.push(this.buildForm(itemFields, engine));
  }

  removeArrayItem(formArray: FormArray, index: number): void {
    formArray.removeAt(index);
  }

  private getDefaultForType(type: ControlType): any {
    switch (type) {
      case 'checkbox': case 'toggle': return false;
      case 'number': case 'slider': return 0;
      default: return '';
    }
  }
}

// Type re-export pour l'injection (on importe DependencyEngine ici
// pour le type signature, le vrai fichier TS importera normalement)
import { DependencyEngine } from './dependency-engine';


// ═══════════════════════════════════════════════════════════════════════════
// FICHIER 4 : dynamic-form.component.ts — Le composant principal
// ═══════════════════════════════════════════════════════════════════════════

// --- Copier dans : libs/<your-lib>/src/lib/dynamic-form/dynamic-form.component.ts ---

import {
  Component, Input, Output, EventEmitter,
  OnInit, OnDestroy, OnChanges, SimpleChanges,
  ChangeDetectionStrategy, ChangeDetectorRef,
  TemplateRef, ContentChildren, QueryList,
  Directive, inject, signal, computed, effect,
  DestroyRef,
} from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule, FormGroup, FormControl, FormArray, AbstractControl } from '@angular/forms';
import { Subscription, startWith, debounceTime } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

// import { DependencyEngine } from './dependency-engine';
// import { DynamicFormBuilderService } from './form-builder.service';
// import type { FormConfig, FieldConfig, ControlType, FieldRuntimeState, SelectOption } from './models';

// ── Directive pour les templates custom ──

@Directive({
  selector: '[dynamicFieldTemplate]',
  standalone: true,
})
export class DynamicFieldTemplateDirective {
  @Input('dynamicFieldTemplate') name!: string;
  constructor(public templateRef: TemplateRef<any>) {}
}


// ── Composant principal ──

@Component({
  selector: 'lib-dynamic-form',
  standalone: true,
  imports: [CommonModule, ReactiveFormsModule],
  providers: [DependencyEngine, DynamicFormBuilderService],
  changeDetection: ChangeDetectionStrategy.OnPush,
  // ⚠️ ANGULAR 17 : pas de @let, pas de @for...track $index natif
  // On utilise @if / @for (nouveau control flow dispo depuis Angular 17)
  // Mais on NE peut PAS utiliser @let (Angular 18.1+)
  template: `
    @if (formGroup) {
      <form
        [formGroup]="formGroup"
        [class]="config.cssClass ?? ''"
        [ngClass]="{
          'df': true,
          'df--sm': config.layout?.size === 'sm',
          'df--lg': config.layout?.size === 'lg',
          'df--label-left': config.layout?.labelPosition === 'left'
        }"
        (ngSubmit)="onSubmit()"
      >
        <!-- Header -->
        @if (config.title || config.description) {
          <div class="df__header">
            @if (config.title) { <h2 class="df__title">{{ config.title }}</h2> }
            @if (config.description) { <p class="df__desc">{{ config.description }}</p> }
          </div>
        }

        <!-- Grille -->
        <div class="df__grid" [style.--df-cols]="config.layout?.columns ?? 1">
          @for (field of config.fields; track field.key) {
            <ng-container
              [ngTemplateOutlet]="fieldTpl"
              [ngTemplateOutletContext]="{ $implicit: field, fg: formGroup, parentPath: '' }"
            ></ng-container>
          }
        </div>

        <!-- Actions -->
        <div class="df__actions">
          @if (config.showReset) {
            <button type="button" class="df__btn df__btn--reset" (click)="onReset()">
              {{ config.resetLabel ?? 'Réinitialiser' }}
            </button>
          }
          <button
            type="submit"
            class="df__btn df__btn--submit"
            [disabled]="config.disableSubmitIfInvalid && formGroup.invalid"
          >
            {{ config.submitLabel ?? 'Soumettre' }}
          </button>
        </div>
      </form>
    }

    <!-- ═══ Template récursif pour chaque champ ═══ -->
    <ng-template #fieldTpl let-field let-fg="fg" let-parentPath="parentPath">
      <!-- On récupère l'état runtime depuis le cache signal -->
      @if (getRuntimeState(field).visible) {
        <!-- Séparateur -->
        @if (field.layout?.separator) {
          <div class="df__separator" style="grid-column: 1 / -1"></div>
        }

        <!-- Section title -->
        @if (field.layout?.sectionTitle) {
          <div class="df__section" style="grid-column: 1 / -1">
            <h3 class="df__section-title">{{ field.layout.sectionTitle }}</h3>
            @if (field.layout.sectionDescription) {
              <p class="df__section-desc">{{ field.layout.sectionDescription }}</p>
            }
          </div>
        }

        <!-- Field wrapper -->
        <div
          class="df__field"
          [ngClass]="[
            'df__field--' + field.type,
            getRuntimeState(field).cssClass
          ]"
          [class.df__field--error]="getCtrl(fg, field.key)?.invalid && getCtrl(fg, field.key)?.touched"
          [class.df__field--disabled]="getRuntimeState(field).disabled"
          [style.grid-column]="'span ' + (field.layout?.colSpan ?? 1)"
          [style.order]="field.layout?.order ?? null"
        >
          <!-- LABEL -->
          @if (field.type !== 'hidden' && field.type !== 'checkbox' && field.type !== 'toggle') {
            <label class="df__label" [attr.for]="fieldId(field, parentPath)">
              {{ getRuntimeState(field).label || field.label }}
              @if (getRuntimeState(field).required) { <span class="df__req">*</span> }
              @if (field.tooltip) { <span class="df__tip" [title]="field.tooltip">ⓘ</span> }
            </label>
          }

          <!-- ═══ TEXT / NUMBER / EMAIL / PASSWORD ═══ -->
          @if (isTextInput(field.type)) {
            <div class="df__input-wrap">
              @if (field.prefix) { <span class="df__prefix">{{ field.prefix }}</span> }
              <input
                [id]="fieldId(field, parentPath)"
                [type]="inputType(field.type)"
                [formControl]="asFormControl(fg, field.key)"
                [placeholder]="getRuntimeState(field).placeholder"
                [attr.min]="getRuntimeState(field).min"
                [attr.max]="getRuntimeState(field).max"
                [readonly]="getRuntimeState(field).readonly"
                class="df__input"
              />
              @if (field.suffix) { <span class="df__suffix">{{ field.suffix }}</span> }
            </div>
          }

          <!-- ═══ TEXTAREA ═══ -->
          @if (field.type === 'textarea') {
            <textarea
              [id]="fieldId(field, parentPath)"
              [formControl]="asFormControl(fg, field.key)"
              [placeholder]="getRuntimeState(field).placeholder"
              [readonly]="getRuntimeState(field).readonly"
              class="df__textarea"
              rows="4"
            ></textarea>
          }

          <!-- ═══ SELECT ═══ -->
          @if (field.type === 'select') {
            <div class="df__select-wrap">
              <select
                [id]="fieldId(field, parentPath)"
                [formControl]="asFormControl(fg, field.key)"
                class="df__select"
              >
                @if (getRuntimeState(field).placeholder) {
                  <option value="" disabled>{{ getRuntimeState(field).placeholder }}</option>
                }
                @for (opt of getRuntimeState(field).options; track opt.value) {
                  <option [value]="opt.value" [disabled]="opt.disabled ?? false">{{ opt.label }}</option>
                }
              </select>
              @if (getRuntimeState(field).loading) {
                <span class="df__spinner"></span>
              }
            </div>
          }

          <!-- ═══ RADIO ═══ -->
          @if (field.type === 'radio') {
            <div class="df__radio-group">
              @for (opt of getRuntimeState(field).options; track opt.value) {
                <label class="df__radio-item">
                  <input type="radio" [formControl]="asFormControl(fg, field.key)"
                         [value]="opt.value" class="df__radio-input" />
                  <span class="df__radio-label">{{ opt.label }}</span>
                </label>
              }
            </div>
          }

          <!-- ═══ CHECKBOX ═══ -->
          @if (field.type === 'checkbox') {
            <label class="df__checkbox-item">
              <input [id]="fieldId(field, parentPath)" type="checkbox"
                     [formControl]="asFormControl(fg, field.key)" class="df__checkbox-input" />
              <span class="df__checkbox-label">
                {{ getRuntimeState(field).label || field.label }}
                @if (getRuntimeState(field).required) { <span class="df__req">*</span> }
              </span>
            </label>
          }

          <!-- ═══ TOGGLE ═══ -->
          @if (field.type === 'toggle') {
            <label class="df__toggle-item">
              <input [id]="fieldId(field, parentPath)" type="checkbox"
                     [formControl]="asFormControl(fg, field.key)" class="df__toggle-input" />
              <span class="df__toggle-slider"></span>
              <span class="df__toggle-label">{{ getRuntimeState(field).label || field.label }}</span>
            </label>
          }

          <!-- ═══ DATE / TIME ═══ -->
          @if (field.type === 'date' || field.type === 'time') {
            <input
              [id]="fieldId(field, parentPath)"
              [type]="field.type"
              [formControl]="asFormControl(fg, field.key)"
              [attr.min]="getRuntimeState(field).min"
              [attr.max]="getRuntimeState(field).max"
              class="df__input"
            />
          }

          <!-- ═══ SLIDER ═══ -->
          @if (field.type === 'slider') {
            <div class="df__slider-wrap">
              <input [id]="fieldId(field, parentPath)" type="range"
                     [formControl]="asFormControl(fg, field.key)"
                     [attr.min]="getRuntimeState(field).min ?? 0"
                     [attr.max]="getRuntimeState(field).max ?? 100"
                     class="df__slider" />
              <span class="df__slider-val">{{ getCtrl(fg, field.key)?.value }}</span>
            </div>
          }

          <!-- ═══ HIDDEN ═══ -->
          @if (field.type === 'hidden') {
            <input type="hidden" [formControl]="asFormControl(fg, field.key)" />
          }

          <!-- ═══ GROUP ═══ -->
          @if (field.type === 'group' && field.children) {
            <div class="df__group" [formGroupName]="field.key">
              <div class="df__grid" [style.--df-cols]="config.layout?.columns ?? 1">
                @for (child of field.children; track child.key) {
                  <ng-container
                    [ngTemplateOutlet]="fieldTpl"
                    [ngTemplateOutletContext]="{
                      $implicit: child,
                      fg: asFormGroup(fg, field.key),
                      parentPath: joinPath(parentPath, field.key)
                    }"
                  ></ng-container>
                }
              </div>
            </div>
          }

          <!-- ═══ ARRAY ═══ -->
          @if (field.type === 'array' && field.arrayConfig) {
            <div class="df__array" [formArrayName]="field.key">
              @for (item of asFormArray(fg, field.key).controls; track $index) {
                <div class="df__array-item" [formGroupName]="$index">
                  <div class="df__grid" [style.--df-cols]="config.layout?.columns ?? 1">
                    @for (child of field.arrayConfig.itemFields; track child.key) {
                      <ng-container
                        [ngTemplateOutlet]="fieldTpl"
                        [ngTemplateOutletContext]="{
                          $implicit: child,
                          fg: asFormGroupAt(fg, field.key, $index),
                          parentPath: joinPath(parentPath, field.key, '' + $index)
                        }"
                      ></ng-container>
                    }
                  </div>
                  @if (!field.arrayConfig.minItems || asFormArray(fg, field.key).length > (field.arrayConfig.minItems ?? 0)) {
                    <button type="button" class="df__btn df__btn--remove"
                            (click)="removeArrayItem(fg, field, $index)">
                      {{ field.arrayConfig.removeLabel ?? '✕ Supprimer' }}
                    </button>
                  }
                </div>
              }
              @if (!field.arrayConfig.maxItems || asFormArray(fg, field.key).length < (field.arrayConfig.maxItems ?? 999)) {
                <button type="button" class="df__btn df__btn--add"
                        (click)="addArrayItem(fg, field)">
                  {{ field.arrayConfig.addLabel ?? '+ Ajouter' }}
                </button>
              }
            </div>
          }

          <!-- ═══ CUSTOM TEMPLATE ═══ -->
          @if (field.type === 'custom' && field.customTemplateRef) {
            @for (tpl of customTemplates; track tpl.name) {
              @if (tpl.name === field.customTemplateRef) {
                <ng-container
                  [ngTemplateOutlet]="tpl.templateRef"
                  [ngTemplateOutletContext]="{
                    $implicit: field,
                    control: getCtrl(fg, field.key),
                    formGroup: fg,
                    formValues: fg.getRawValue()
                  }"
                ></ng-container>
              }
            }
          }

          <!-- Help text -->
          @if (getRuntimeState(field).helpText && field.type !== 'hidden') {
            <small class="df__help">{{ getRuntimeState(field).helpText }}</small>
          }

          <!-- Error -->
          @if (getCtrl(fg, field.key)?.invalid && getCtrl(fg, field.key)?.touched && field.type !== 'hidden') {
            <small class="df__error">{{ getError(field, fg) }}</small>
          }
        </div>
      }
    </ng-template>
  `,
  styles: [`
    :host {
      --df-primary: #4f46e5;
      --df-primary-h: #4338ca;
      --df-danger: #ef4444;
      --df-bg: #fff;
      --df-surface: #f8fafc;
      --df-border: #e2e8f0;
      --df-text: #1e293b;
      --df-muted: #64748b;
      --df-placeholder: #94a3b8;
      --df-radius: 8px;
      --df-gap: 1rem;
      --df-font: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
      --df-tr: 200ms cubic-bezier(.4,0,.2,1);
      display: block; font-family: var(--df-font); color: var(--df-text);
    }
    .df__header { margin-bottom: calc(var(--df-gap)*1.5); }
    .df__title { font-size: 1.5rem; font-weight: 700; margin: 0 0 .25rem; }
    .df__desc { font-size: .875rem; color: var(--df-muted); margin: 0; }
    .df__grid { display: grid; grid-template-columns: repeat(var(--df-cols,1),1fr); gap: var(--df-gap); align-items: start; }
    .df__field { display: flex; flex-direction: column; gap: .375rem; }
    .df__label { font-size: .875rem; font-weight: 600; display: flex; align-items: center; gap: .25rem; }
    .df__req { color: var(--df-danger); font-weight: 700; }
    .df__tip { cursor: help; opacity: .5; font-size: .75rem; }
    .df--label-left .df__field { flex-direction: row; align-items: center; gap: var(--df-gap); }
    .df--label-left .df__label { min-width: 150px; flex-shrink: 0; }
    .df__input,.df__textarea,.df__select {
      width: 100%; padding: .625rem .875rem; font-size: .875rem; font-family: inherit;
      color: var(--df-text); background: var(--df-bg); border: 1.5px solid var(--df-border);
      border-radius: var(--df-radius); transition: border-color var(--df-tr), box-shadow var(--df-tr);
      outline: none; box-sizing: border-box;
    }
    .df__input:focus,.df__textarea:focus,.df__select:focus {
      border-color: var(--df-primary); box-shadow: 0 0 0 3px rgba(79,70,229,.1);
    }
    .df__input::placeholder,.df__textarea::placeholder { color: var(--df-placeholder); }
    .df__input:disabled,.df__textarea:disabled,.df__select:disabled {
      opacity: .6; cursor: not-allowed; background: var(--df-surface);
    }
    .df__input:read-only { background: var(--df-surface); }
    .df__field--error .df__input,.df__field--error .df__textarea,.df__field--error .df__select {
      border-color: var(--df-danger);
    }
    .df__input-wrap { display: flex; align-items: center; }
    .df__input-wrap .df__input { flex: 1; }
    .df__prefix,.df__suffix {
      padding: .625rem .75rem; font-size: .875rem; color: var(--df-muted);
      background: var(--df-surface); border: 1.5px solid var(--df-border); white-space: nowrap;
    }
    .df__prefix { border-right: none; border-radius: var(--df-radius) 0 0 var(--df-radius); }
    .df__prefix+.df__input { border-radius: 0 var(--df-radius) var(--df-radius) 0; }
    .df__suffix { border-left: none; border-radius: 0 var(--df-radius) var(--df-radius) 0; }
    .df__select-wrap { position: relative; }
    .df__select {
      appearance: none; padding-right: 2.5rem; cursor: pointer;
      background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='16' height='16' viewBox='0 0 24 24' fill='none' stroke='%2364748b' stroke-width='2'%3E%3Cpath d='M6 9l6 6 6-6'/%3E%3C/svg%3E");
      background-repeat: no-repeat; background-position: right .75rem center;
    }
    .df__spinner {
      position: absolute; right: 2.5rem; top: 50%; transform: translateY(-50%);
      width: 16px; height: 16px; border: 2px solid var(--df-border); border-top-color: var(--df-primary);
      border-radius: 50%; animation: df-spin 1s linear infinite;
    }
    @keyframes df-spin { to { transform: translateY(-50%) rotate(360deg); } }
    .df__radio-group { display: flex; flex-wrap: wrap; gap: .75rem; }
    .df__radio-item { display: flex; align-items: center; gap: .5rem; cursor: pointer; font-size: .875rem; }
    .df__radio-input { accent-color: var(--df-primary); width: 1rem; height: 1rem; }
    .df__checkbox-item { display: flex; align-items: center; gap: .5rem; cursor: pointer; font-size: .875rem; }
    .df__checkbox-input { accent-color: var(--df-primary); width: 1.125rem; height: 1.125rem; }
    .df__toggle-item { display: flex; align-items: center; gap: .75rem; cursor: pointer; font-size: .875rem; }
    .df__toggle-input { display: none; }
    .df__toggle-slider {
      width: 44px; height: 24px; background: var(--df-border); border-radius: 12px;
      position: relative; transition: background var(--df-tr); flex-shrink: 0;
    }
    .df__toggle-slider::after {
      content: ''; position: absolute; top: 2px; left: 2px; width: 20px; height: 20px;
      background: #fff; border-radius: 50%; transition: transform var(--df-tr);
      box-shadow: 0 1px 3px rgba(0,0,0,.2);
    }
    .df__toggle-input:checked+.df__toggle-slider { background: var(--df-primary); }
    .df__toggle-input:checked+.df__toggle-slider::after { transform: translateX(20px); }
    .df__slider-wrap { display: flex; align-items: center; gap: .75rem; }
    .df__slider { flex: 1; accent-color: var(--df-primary); height: 6px; }
    .df__slider-val { font-size: .875rem; font-weight: 600; min-width: 2rem; text-align: right; }
    .df__group,.df__array-item {
      border: 1.5px solid var(--df-border); border-radius: var(--df-radius);
      padding: var(--df-gap); background: var(--df-surface);
    }
    .df__array-item { margin-bottom: .75rem; }
    .df__help { font-size: .75rem; color: var(--df-muted); }
    .df__error { font-size: .75rem; color: var(--df-danger); font-weight: 500; }
    .df__separator { border-top: 1px solid var(--df-border); margin: .5rem 0; }
    .df__section { margin: .5rem 0 0; }
    .df__section-title { font-size: 1.125rem; font-weight: 700; margin: 0 0 .125rem; }
    .df__section-desc { font-size: .8125rem; color: var(--df-muted); margin: 0; }
    .df__actions {
      display: flex; justify-content: flex-end; gap: .75rem;
      margin-top: calc(var(--df-gap)*1.5); padding-top: var(--df-gap);
      border-top: 1px solid var(--df-border);
    }
    .df__btn {
      padding: .625rem 1.5rem; font-size: .875rem; font-weight: 600; font-family: inherit;
      border-radius: var(--df-radius); border: none; cursor: pointer; transition: all var(--df-tr);
    }
    .df__btn--submit { background: var(--df-primary); color: #fff; }
    .df__btn--submit:hover:not(:disabled) { background: var(--df-primary-h); }
    .df__btn--submit:disabled { opacity: .5; cursor: not-allowed; }
    .df__btn--reset { background: transparent; color: var(--df-muted); border: 1.5px solid var(--df-border); }
    .df__btn--reset:hover { background: var(--df-surface); }
    .df__btn--add { background: transparent; color: var(--df-primary); border: 1.5px dashed var(--df-primary); width: 100%; padding: .5rem; }
    .df__btn--add:hover { background: rgba(79,70,229,.05); }
    .df__btn--remove { background: transparent; color: var(--df-danger); border: 1px solid var(--df-danger); padding: .375rem .75rem; font-size: .8125rem; margin-top: .5rem; align-self: flex-end; }
    .df--sm .df__input,.df--sm .df__textarea,.df--sm .df__select { padding: .4375rem .625rem; font-size: .8125rem; }
    .df--lg .df__input,.df--lg .df__textarea,.df--lg .df__select { padding: .8125rem 1rem; font-size: 1rem; }
    @media(max-width:640px) {
      .df__grid { grid-template-columns: 1fr !important; }
      .df__field { grid-column: span 1 !important; }
      .df--label-left .df__field { flex-direction: column; }
    }
  `],
})
export class DynamicFormComponent implements OnInit, OnDestroy, OnChanges {

  // ⚠️ Angular 17 : @Input() classique, pas de signal inputs
  @Input() config!: FormConfig;

  @Output() formSubmit = new EventEmitter<Record<string, any>>();
  @Output() formValueChange = new EventEmitter<Record<string, any>>();
  @Output() formStatusChange = new EventEmitter<string>();

  @ContentChildren(DynamicFieldTemplateDirective)
  customTemplates!: QueryList<DynamicFieldTemplateDirective>;

  formGroup!: FormGroup;

  private engine = inject(DependencyEngine);
  private formBuilder = inject(DynamicFormBuilderService);
  private cdr = inject(ChangeDetectorRef);
  private destroyRef = inject(DestroyRef);

  // Signal interne pour stocker les valeurs du formulaire
  // → Sert de source réactive pour les computed() du DependencyEngine
  private formValues = signal<Record<string, any>>({});

  // Cache des états runtime (computed signals, recalculés automatiquement)
  private runtimeCache = new Map<string, ReturnType<typeof computed<FieldRuntimeState>>>();

  // Cache des options async
  private asyncOptionsCache = new Map<string, { options: ReturnType<typeof signal<SelectOption[]>>; loading: ReturnType<typeof signal<boolean>> }>();

  // Pour stocker les states précédents (éviter les updates inutiles)
  private previousStates = new Map<string, FieldRuntimeState>();

  private valueChangeSub?: Subscription;

  ngOnInit(): void {
    this.buildForm();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['config'] && !changes['config'].firstChange) {
      this.runtimeCache.clear();
      this.asyncOptionsCache.clear();
      this.previousStates.clear();
      this.buildForm();
    }
  }

  ngOnDestroy(): void {
    this.valueChangeSub?.unsubscribe();
  }

  // ─── Construction du formulaire ───

  private buildForm(): void {
    this.valueChangeSub?.unsubscribe();

    this.formGroup = this.formBuilder.buildForm(this.config.fields, this.engine);

    // Initialiser les computed signals pour chaque champ
    this.initRuntimeCache(this.config.fields);

    // Charger les options async
    this.initAsyncOptions(this.config.fields);

    // Écouter valueChanges → mettre à jour le signal formValues
    this.valueChangeSub = this.formGroup.valueChanges.pipe(
      startWith(this.formGroup.getRawValue()),
      debounceTime(30),
    ).subscribe(() => {
      const raw = this.formGroup.getRawValue();
      this.formValues.set(raw);
      this.formValueChange.emit(raw);

      // Appliquer les effets de bord (disable/enable, setValue, resetValue)
      this.applyAllEffects(this.config.fields, raw);
      this.cdr.markForCheck();
    });

    // Status
    this.formGroup.statusChanges.pipe(
      takeUntilDestroyed(this.destroyRef),
    ).subscribe(status => this.formStatusChange.emit(status));

    // Process initial
    const initial = this.formGroup.getRawValue();
    this.formValues.set(initial);
    this.applyAllEffects(this.config.fields, initial);
  }

  private initRuntimeCache(fields: FieldConfig[], parentPath = ''): void {
    for (const field of fields) {
      const key = parentPath ? `${parentPath}.${field.key}` : field.key;

      // Computed signal : se recalcule quand formValues() change
      this.runtimeCache.set(key, computed(() => {
        const values = this.formValues();
        const state = this.engine.computeFieldState(field, values);

        // Fusionner les options async si disponibles
        const asyncCache = this.asyncOptionsCache.get(key);
        if (asyncCache) {
          const asyncOpts = asyncCache.options();
          if (asyncOpts.length > 0) state.options = asyncOpts;
          state.loading = asyncCache.loading();
        }

        return state;
      }));

      if (field.type === 'group' && field.children) {
        this.initRuntimeCache(field.children, key);
      }
      if (field.type === 'array' && field.arrayConfig) {
        this.initRuntimeCache(field.arrayConfig.itemFields, key);
      }
    }
  }

  private initAsyncOptions(fields: FieldConfig[], parentPath = ''): void {
    for (const field of fields) {
      const key = parentPath ? `${parentPath}.${field.key}` : field.key;

      // Loader initial
      if (field.optionsLoader) {
        this.createAsyncLoader(key, field.optionsLoader);
      }

      // Loaders dans les dépendances
      if (field.dependencies) {
        for (const dep of field.dependencies) {
          if (dep.effects.optionsLoader) {
            this.createAsyncLoader(key, dep.effects.optionsLoader, dep);
          }
        }
      }

      if (field.children) this.initAsyncOptions(field.children, key);
    }
  }

  private createAsyncLoader(
    key: string,
    loader: (fv: Record<string, any>) => Promise<SelectOption[]>,
    rule?: DependencyRule
  ): void {
    if (!this.asyncOptionsCache.has(key)) {
      this.asyncOptionsCache.set(key, {
        options: signal<SelectOption[]>([]),
        loading: signal(false),
      });
    }

    const cache = this.asyncOptionsCache.get(key)!;
    let lastTrigger = '';

    // On utilise effect() pour réagir aux changements de formValues
    effect(() => {
      const values = this.formValues();

      // Si une règle est associée, ne charger que si la condition est remplie
      if (rule && !this.engine.evaluateRule(rule, values)) return;

      // Déterminer si les dépendances sources ont changé
      const trigger = rule
        ? JSON.stringify(rule.conditions.map(c => this.engine.getNestedValue(values, c.field)))
        : JSON.stringify(values);

      if (trigger !== lastTrigger) {
        lastTrigger = trigger;
        cache.loading.set(true);
        loader(values).then(opts => {
          cache.options.set(opts);
          cache.loading.set(false);
          this.cdr.markForCheck();
        }).catch(() => {
          cache.options.set([]);
          cache.loading.set(false);
          this.cdr.markForCheck();
        });
      }
    }, { allowSignalWrites: true });
  }

  private applyAllEffects(fields: FieldConfig[], formValues: Record<string, any>): void {
    for (const field of fields) {
      const key = field.key;
      const stateSignal = this.runtimeCache.get(key);
      if (!stateSignal) continue;

      const state = stateSignal();
      const prev = this.previousStates.get(key);

      // Appliquer disable/enable et validators sur le FormControl
      this.engine.applyControlEffects(field, state, this.formGroup, formValues, prev);

      // Appliquer setValue / resetValue
      this.engine.applySideEffects(field, this.formGroup, formValues);

      this.previousStates.set(key, { ...state });

      // Récursion
      if (field.type === 'group' && field.children) {
        this.applyAllEffects(field.children, formValues);
      }
    }
  }


  // ─── API publique ───

  onSubmit(): void {
    if (this.formGroup.valid) {
      this.formSubmit.emit(this.formGroup.getRawValue());
    } else {
      this.markAllTouched(this.formGroup);
      this.cdr.markForCheck();
    }
  }

  onReset(): void {
    const defaults = this.getDefaults(this.config.fields);
    this.formGroup.reset(defaults);
  }

  /** Accès programmatique au FormGroup */
  getFormGroup(): FormGroup { return this.formGroup; }

  /** Mise à jour programmatique */
  patchValue(values: Record<string, any>): void {
    this.formGroup.patchValue(values);
  }


  // ─── Template helpers ───

  /**
   * Retourne l'état runtime d'un champ.
   * Utilise le cache computed signal → recalcul automatique quand les valeurs changent.
   */
  getRuntimeState(field: FieldConfig): FieldRuntimeState {
    const cached = this.runtimeCache.get(field.key);
    if (cached) return cached();

    // Fallback (ne devrait pas arriver en fonctionnement normal)
    return this.engine.computeFieldState(field, this.formValues());
  }

  isTextInput(type: ControlType): boolean {
    return ['text', 'number', 'email', 'password'].includes(type);
  }

  inputType(type: ControlType): string {
    switch (type) {
      case 'number': return 'number';
      case 'email':  return 'email';
      case 'password': return 'password';
      default: return 'text';
    }
  }

  fieldId(field: FieldConfig, parentPath: string): string {
    return parentPath ? `${parentPath}_${field.key}` : field.key;
  }

  joinPath(...parts: string[]): string {
    return parts.filter(Boolean).join('.');
  }

  getCtrl(fg: FormGroup, key: string): AbstractControl | null {
    return fg.get(key);
  }

  asFormControl(fg: FormGroup, key: string): FormControl {
    return fg.get(key) as FormControl;
  }

  asFormGroup(fg: FormGroup, key: string): FormGroup {
    return fg.get(key) as FormGroup;
  }

  asFormArray(fg: FormGroup, key: string): FormArray {
    return fg.get(key) as FormArray;
  }

  asFormGroupAt(fg: FormGroup, key: string, idx: number): FormGroup {
    return (fg.get(key) as FormArray).at(idx) as FormGroup;
  }

  getError(field: FieldConfig, fg: FormGroup): string | null {
    const ctrl = fg.get(field.key);
    return ctrl ? this.engine.getErrorMessage(field, ctrl) : null;
  }

  addArrayItem(fg: FormGroup, field: FieldConfig): void {
    if (!field.arrayConfig) return;
    const arr = fg.get(field.key) as FormArray;
    this.formBuilder.addArrayItem(arr, field.arrayConfig.itemFields, this.engine);
    this.cdr.markForCheck();
  }

  removeArrayItem(fg: FormGroup, field: FieldConfig, index: number): void {
    const arr = fg.get(field.key) as FormArray;
    this.formBuilder.removeArrayItem(arr, index);
    this.cdr.markForCheck();
  }

  // ─── Utilitaires privés ───

  private markAllTouched(control: AbstractControl): void {
    if (control instanceof FormControl) {
      control.markAsTouched();
    } else if (control instanceof FormGroup) {
      Object.values(control.controls).forEach(c => this.markAllTouched(c));
    } else if (control instanceof FormArray) {
      control.controls.forEach(c => this.markAllTouched(c));
    }
  }

  private getDefaults(fields: FieldConfig[]): Record<string, any> {
    const d: Record<string, any> = {};
    for (const f of fields) {
      if (f.type === 'group' && f.children) {
        d[f.key] = this.getDefaults(f.children);
      } else {
        d[f.key] = f.defaultValue ?? null;
      }
    }
    return d;
  }
}
