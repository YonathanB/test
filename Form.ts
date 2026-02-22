// libs/dynamic-form/src/lib/form-builder.service.ts

import { Injectable, inject } from '@angular/core';
import { FormBuilder, FormGroup, FormControl, FormArray, type AbstractControl } from '@angular/forms';
import type { FieldConfig, ControlType } from './models';
import { DependencyEngine } from './dependency-engine';

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
        group[field.key] = this.fb.array(
          initialItems.map(() => this.buildForm(field.arrayConfig!.itemFields, engine))
        );
      } else {
        const control = new FormControl(
          { value: field.defaultValue ?? this.getDefaultForType(field.type), disabled: field.disabled || false },
          engine.buildValidators(field)
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
