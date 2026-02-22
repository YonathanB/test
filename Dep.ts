// libs/dynamic-form/src/lib/dependency-engine.ts

import { Injectable, isSignal } from '@angular/core';
import { Validators, type ValidatorFn, type AbstractControl, FormGroup } from '@angular/forms';
import type { FieldConfig, FieldRuntimeState, DependencyCondition, DependencyRule, DependencyEffect } from './models';

@Injectable()
export class DependencyEngine {

  evaluateCondition(condition: DependencyCondition, formValues: Record<string, any>): boolean {
    const fieldValue = this.getNestedValue(formValues, condition.field);

    switch (condition.operator) {
      case 'equals': return fieldValue === condition.value;
      case 'notEquals': return fieldValue !== condition.value;
      case 'contains': return fieldValue?.includes?.(condition.value) ?? false;
      case 'isEmpty': return fieldValue == null || fieldValue === '' || (Array.isArray(fieldValue) && fieldValue.length === 0);
      case 'isNotEmpty': return !this.evaluateCondition({ ...condition, operator: 'isEmpty' }, formValues);
      case 'in': return Array.isArray(condition.value) && condition.value.includes(fieldValue);
      // ... (Ajoute tes autres opérateurs personnalisés ici si besoin)
      default: return false;
    }
  }

  evaluateRule(rule: DependencyRule, formValues: Record<string, any>): boolean {
    const logic = rule.logic || 'AND';
    return logic === 'AND'
      ? rule.conditions.every(c => this.evaluateCondition(c, formValues))
      : rule.conditions.some(c => this.evaluateCondition(c, formValues));
  }

  computeFieldState(field: FieldConfig, formValues: Record<string, any>): FieldRuntimeState {
    const state: FieldRuntimeState = {
      type: field.type, // 💡 Initialise avec le type par défaut
      visible: !field.hidden,
      disabled: field.disabled || false,
      required: field.validators?.some(v => v.type === 'required') || false,
      readonly: field.readonly || false,
      placeholder: field.placeholder || '',
      label: field.label || '',
      // 💡 ARCHITECTURE SIGNAL : On unwrap le signal de base, l'auto-tracking d'Angular fait le reste.
      // Les observables seront traités séparément par le composant visuel.
      options: isSignal(field.options) ? field.options() : (Array.isArray(field.options) ? [...field.options] : []),
      dataSource: field.dataSource,
      helpText: field.helpText || '',
      cssClass: field.cssClass || '',
      loading: false,
    };

    if (field.dependencies) {
      for (const rule of field.dependencies) {
        if (this.evaluateRule(rule, formValues)) {
          this.mergeEffects(state, rule.effects, formValues);
        } else if (rule.elseEffects) {
          this.mergeEffects(state, rule.elseEffects, formValues);
        }
      }
    }
    return state;
  }

  private mergeEffects(state: FieldRuntimeState, effects: DependencyEffect, formValues: Record<string, any>): void {
    if (effects.type !== undefined) state.type = effects.type; // 💡 Surcharge dynamique du type
    if (effects.visible !== undefined) state.visible = effects.visible;
    if (effects.disabled !== undefined) state.disabled = effects.disabled;
    if (effects.required !== undefined) state.required = effects.required;
    if (effects.label !== undefined) state.label = effects.label;
    if (effects.helpText !== undefined) state.helpText = effects.helpText;
    
    // 💡 Surcharge dynamique du provider Custom Autocomplete
    if (effects.dataSource !== undefined) {
      state.dataSource = typeof effects.dataSource === 'function' ? effects.dataSource(formValues) : effects.dataSource;
    }

    if (effects.options !== undefined) {
      const result = typeof effects.options === 'function' ? effects.options(formValues) : effects.options;
      if (isSignal(result)) state.options = result();
      else if (Array.isArray(result)) state.options = result;
    }
  }

  // Applique les propriétés "physiques" sur l'instance du AbstractControl
  applyControlEffects(field: FieldConfig, state: FieldRuntimeState, formGroup: FormGroup, prev?: FieldRuntimeState): void {
    const control = formGroup.get(field.key);
    if (!control) return;

    if (!prev || prev.disabled !== state.disabled) {
      state.disabled ? control.disable({ emitEvent: false }) : control.enable({ emitEvent: false });
    }
    if (!prev || prev.required !== state.required) {
      control.setValidators(this.buildValidators(field, state));
      control.updateValueAndValidity({ emitEvent: false });
    }
  }

  // Applique les changements de valeurs (setValue / resetValue)
  applySideEffects(field: FieldConfig, formGroup: FormGroup, formValues: Record<string, any>): void {
    if (!field.dependencies) return;
    const control = formGroup.get(field.key);
    if (!control) return;

    for (const rule of field.dependencies) {
      const effects = this.evaluateRule(rule, formValues) ? rule.effects : rule.elseEffects;
      if (!effects) continue;

      if (effects.setValue !== undefined) {
        const newVal = typeof effects.setValue === 'function' ? effects.setValue(formValues) : effects.setValue;
        if (control.value !== newVal) control.setValue(newVal, { emitEvent: false });
      }
      if (effects.resetValue && control.value !== (field.defaultValue ?? null)) {
        control.reset(field.defaultValue ?? null, { emitEvent: false });
      }
    }
  }

  buildValidators(field: FieldConfig, state?: FieldRuntimeState): ValidatorFn[] {
    const validators: ValidatorFn[] = [];
    if (state?.required) validators.push(Validators.required);
    // (Ajoute ta boucle for sur field.validators ici)
    return validators;
  }

  getErrorMessage(field: FieldConfig, control: AbstractControl): string | null {
    if (!control.errors || !control.touched) return null;
    if (control.errors['required']) return 'Ce champ est requis';
    return 'Valeur invalide';
  }

  getNestedValue(obj: any, path: string): any {
    return path.split('.').reduce((acc, key) => acc?.[key], obj);
  }
}
