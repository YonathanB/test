// libs/dynamic-form/src/lib/models.ts

import type { Signal } from '@angular/core';
import type { Observable } from 'rxjs';

// 💡 NOUVEAU : Interface pour ton composant Autocomplete externe
export interface CustomAutocompleteProvider {
  getData(txt: string): Observable<any[]>;
}

export type ControlType =
  | 'text' | 'number' | 'email' | 'password' | 'textarea'
  | 'select' | 'autocomplete' | 'radio' | 'checkbox' | 'date'
  | 'time' | 'toggle' | 'slider' | 'file' | 'hidden'
  | 'group' | 'array' | 'custom';

export type DependencyOperator =
  | 'equals' | 'notEquals' | 'contains' | 'notContains'
  | 'greaterThan' | 'lessThan' | 'in' | 'notIn'
  | 'isEmpty' | 'isNotEmpty' | 'matches' | 'custom';

export type ConditionLogic = 'AND' | 'OR';

export interface SelectOption {
  label: string;
  value: any;
  disabled?: boolean;
}

export interface DependencyCondition {
  field: string;
  operator: DependencyOperator;
  value?: any;
  customFn?: (fieldValue: any, formValues: Record<string, any>) => boolean;
}

export interface DependencyEffect {
  // 💡 NOUVEAU : Permet de transformer un 'select' en 'text' dynamiquement
  type?: ControlType; 
  visible?: boolean;
  disabled?: boolean;
  required?: boolean;
  readonly?: boolean;
  placeholder?: string;
  label?: string;
  
  // 💡 NOUVEAU : Support natif des Tableaux, Observables et Signals
  options?: SelectOption[] 
          | Observable<SelectOption[]> 
          | Signal<SelectOption[]>
          | ((formValues: Record<string, any>) => SelectOption[] | Observable<SelectOption[]> | Signal<SelectOption[]>);
  
  optionsLoader?: (formValues: Record<string, any>) => Promise<SelectOption[]>; // Gardé pour rétrocompatibilité
  
  // 💡 NOUVEAU : Injection dynamique de la référence de ton provider custom
  dataSource?: CustomAutocompleteProvider 
             | ((formValues: Record<string, any>) => CustomAutocompleteProvider);

  setValue?: any | ((formValues: Record<string, any>) => any);
  resetValue?: boolean;
  helpText?: string;
  cssClass?: string;
  min?: number | string;
  max?: number | string;
}

export interface DependencyRule {
  id?: string;
  conditions: DependencyCondition[];
  logic?: ConditionLogic;
  effects: DependencyEffect;
  elseEffects?: DependencyEffect;
}

export interface ValidatorConfig {
  type: 'required' | 'min' | 'max' | 'minLength' | 'maxLength' | 'pattern' | 'email' | 'custom';
  value?: any;
  message: string;
  customFn?: (value: any, formValues: Record<string, any>) => boolean;
}

export interface FieldRuntimeState {
  type: ControlType; // Évalué à l'instant T
  visible: boolean;
  disabled: boolean;
  required: boolean;
  readonly: boolean;
  placeholder: string;
  label: string;
  options: SelectOption[];
  dataSource?: CustomAutocompleteProvider; // 💡 L'objet final calculé pour ton custom template
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

  // Polymorphisme des options initiales
  options?: SelectOption[] | Observable<SelectOption[]> | Signal<SelectOption[]>;
  optionsLoader?: (formValues: Record<string, any>) => Promise<SelectOption[]>;
  
  dataSource?: CustomAutocompleteProvider; // Provider initial

  validators?: ValidatorConfig[];
  dependencies?: DependencyRule[];
  layout?: { colSpan?: number; order?: number; separator?: boolean; sectionTitle?: string; sectionDescription?: string; };

  children?: FieldConfig[];
  arrayConfig?: { itemFields: FieldConfig[]; minItems?: number; maxItems?: number; addLabel?: string; removeLabel?: string; };

  customTemplateRef?: string;
  cssClass?: string;
}

export interface FormConfig {
  id?: string;
  title?: string;
  description?: string;
  fields: FieldConfig[];
  layout?: { columns?: number; };
  submitLabel?: string;
  showReset?: boolean;
  resetLabel?: string;
  disableSubmitIfInvalid?: boolean;
  hideActions?: boolean; // 💡 NOUVEAU : Pour l'orchestration multi-fragments (désactive les boutons internes)
  cssClass?: string;
}
