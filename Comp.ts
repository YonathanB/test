// libs/dynamic-form/src/lib/dynamic-form.component.ts

import { 
  Component, Input, Output, EventEmitter, OnInit, OnDestroy, OnChanges, SimpleChanges, 
  ChangeDetectionStrategy, ChangeDetectorRef, TemplateRef, ContentChildren, QueryList, 
  Directive, inject, signal, computed, effect, DestroyRef 
} from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule, FormGroup, FormControl, FormArray, AbstractControl } from '@angular/forms';
import { Subscription, startWith, debounceTime, isObservable, Observable } from 'rxjs';

// Imports Angular Material
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatAutocompleteModule } from '@angular/material/autocomplete';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatRadioModule } from '@angular/material/radio';
import { MatSlideToggleModule } from '@angular/material/slide-toggle';
import { MatSliderModule } from '@angular/material/slider';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';

import { DependencyEngine } from './dependency-engine';
import { DynamicFormBuilderService } from './form-builder.service';
import type { FormConfig, FieldConfig, ControlType, FieldRuntimeState, SelectOption } from './models';

// Directive pour accrocher ton Custom Autocomplete depuis l'extérieur
@Directive({ selector: '[dynamicFieldTemplate]', standalone: true })
export class DynamicFieldTemplateDirective {
  @Input('dynamicFieldTemplate') name!: string;
  constructor(public templateRef: TemplateRef<any>) {}
}

@Component({
  selector: 'lib-dynamic-form',
  standalone: true,
  imports: [
    CommonModule, ReactiveFormsModule, MatFormFieldModule, MatInputModule, 
    MatSelectModule, MatAutocompleteModule, MatCheckboxModule, MatRadioModule, 
    MatSlideToggleModule, MatSliderModule, MatButtonModule, MatIconModule, 
    MatTooltipModule, MatProgressSpinnerModule
  ],
  providers: [DependencyEngine, DynamicFormBuilderService],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    @if (formGroup) {
      <form [formGroup]="formGroup" [class]="config.cssClass ?? ''" class="df" (ngSubmit)="onSubmit()">
        
        @if (config.title || config.description) {
          <div class="df__header">
            @if (config.title) { <h2 class="mat-headline-5">{{ config.title }}</h2> }
            @if (config.description) { <p class="mat-body-1 df__muted">{{ config.description }}</p> }
          </div>
        }

        <div class="df__grid" [style.--df-cols]="config.layout?.columns ?? 1">
          @for (field of config.fields; track field.key) {
            <ng-container [ngTemplateOutlet]="fieldTpl" [ngTemplateOutletContext]="{ $implicit: field, fg: formGroup, parentPath: '' }"></ng-container>
          }
        </div>

        @if (!config.hideActions) {
          <div class="df__actions">
            @if (config.showReset) {
              <button mat-stroked-button type="button" (click)="onReset()">{{ config.resetLabel ?? 'Réinitialiser' }}</button>
            }
            <button mat-flat-button color="primary" type="submit" [disabled]="config.disableSubmitIfInvalid && formGroup.invalid">
              {{ config.submitLabel ?? 'Soumettre' }}
            </button>
          </div>
        }
      </form>
    }

    <ng-template #fieldTpl let-field let-fg="fg" let-parentPath="parentPath">
      @if (getRuntimeState(field).visible) {
        
        <div class="df__field-wrapper" [class]="getRuntimeState(field).cssClass" [style.grid-column]="'span ' + (field.layout?.colSpan ?? 1)">
          
          @if (isTextInput(getRuntimeState(field).type) || getRuntimeState(field).type === 'date' || getRuntimeState(field).type === 'time') {
            <mat-form-field appearance="outline" class="df__full-width">
              <mat-label>{{ getRuntimeState(field).label || field.label }}</mat-label>
              <input matInput [id]="fieldId(field, parentPath)" [type]="inputType(getRuntimeState(field).type)"
                     [formControl]="asFormControl(fg, field.key)" [placeholder]="getRuntimeState(field).placeholder"
                     [attr.min]="getRuntimeState(field).min" [attr.max]="getRuntimeState(field).max"
                     [readonly]="getRuntimeState(field).readonly" [required]="getRuntimeState(field).required" />
              <ng-container *ngTemplateOutlet="materialAccessories; context: { field: field, fg: fg }"></ng-container>
            </mat-form-field>
          }

          @if (getRuntimeState(field).type === 'select') {
            <mat-form-field appearance="outline" class="df__full-width">
              <mat-label>{{ getRuntimeState(field).label || field.label }}</mat-label>
              <mat-select [id]="fieldId(field, parentPath)" [formControl]="asFormControl(fg, field.key)" [required]="getRuntimeState(field).required">
                @if (getRuntimeState(field).placeholder) { <mat-option [value]="null" disabled>{{ getRuntimeState(field).placeholder }}</mat-option> }
                @for (opt of getRuntimeState(field).options; track opt.value) {
                  <mat-option [value]="opt.value" [disabled]="opt.disabled ?? false">{{ opt.label }}</mat-option>
                }
              </mat-select>
              <ng-container *ngTemplateOutlet="materialAccessories; context: { field: field, fg: fg }"></ng-container>
            </mat-form-field>
          }

          @if (getRuntimeState(field).type === 'checkbox') {
            <div class="df__standalone-control">
              <mat-checkbox [id]="fieldId(field, parentPath)" [formControl]="asFormControl(fg, field.key)" [required]="getRuntimeState(field).required">
                {{ getRuntimeState(field).label || field.label }}
              </mat-checkbox>
            </div>
          }

          @if (getRuntimeState(field).type === 'custom' && field.customTemplateRef) {
            @for (tpl of customTemplates; track tpl.name) {
              @if (tpl.name === field.customTemplateRef) {
                <ng-container
                  [ngTemplateOutlet]="tpl.templateRef"
                  [ngTemplateOutletContext]="{
                    $implicit: field,
                    control: getCtrl(fg, field.key),
                    formGroup: fg,
                    dataSource: getRuntimeState(field).dataSource 
                    // ^ Injection de la source de données dynamique !
                  }"
                ></ng-container>
              }
            }
          }

          </div>
      }
    </ng-template>

    <ng-template #materialAccessories let-field="field" let-fg="fg">
      @if (field.prefix) { <span matTextPrefix>{{ field.prefix }}&nbsp;</span> }
      @if (field.suffix) { <span matTextSuffix>&nbsp;{{ field.suffix }}</span> }
      @if (field.tooltip) { <mat-icon matSuffix [matTooltip]="field.tooltip" class="df__tooltip-icon">info</mat-icon> }
      @if (getRuntimeState(field).loading) { <mat-spinner matSuffix diameter="20"></mat-spinner> }
      @if (getRuntimeState(field).helpText) { <mat-hint>{{ getRuntimeState(field).helpText }}</mat-hint> }
      @if (getCtrl(fg, field.key)?.invalid && getCtrl(fg, field.key)?.touched) {
        <mat-error>{{ getError(field, fg) }}</mat-error>
      }
    </ng-template>
  `,
  styles: [`
    :host { display: block; --df-gap: 16px; }
    .df__muted { color: var(--mat-sys-on-surface-variant, rgba(0,0,0,0.6)); margin: 0; }
    .df__header { margin-bottom: 24px; }
    .df__grid { display: grid; grid-template-columns: repeat(var(--df-cols, 1), 1fr); gap: var(--df-gap); align-items: start; }
    .df__full-width { width: 100%; }
    .df__standalone-control { display: flex; flex-direction: column; padding: 8px 0 16px 0; }
    .df__actions { display: flex; justify-content: flex-end; gap: 12px; margin-top: 24px; padding-top: 16px; }
    .df__tooltip-icon { opacity: 0.6; cursor: help; font-size: 20px; width: 20px; height: 20px; }
    @media(max-width: 640px) { .df__grid { grid-template-columns: 1fr !important; } .df__field-wrapper { grid-column: span 1 !important; } }
  `]
})
export class DynamicFormComponent implements OnInit, OnDestroy, OnChanges {
  @Input() config!: FormConfig;
  @Output() formSubmit = new EventEmitter<Record<string, any>>();
  @Output() formValueChange = new EventEmitter<Record<string, any>>();
  
  @ContentChildren(DynamicFieldTemplateDirective)
  customTemplates!: QueryList<DynamicFieldTemplateDirective>;

  formGroup!: FormGroup;

  private engine = inject(DependencyEngine);
  private formBuilder = inject(DynamicFormBuilderService);
  private cdr = inject(ChangeDetectorRef);
  private destroyRef = inject(DestroyRef);

  private formValues = signal<Record<string, any>>({});
  private runtimeCache = new Map<string, ReturnType<typeof computed<FieldRuntimeState>>>();
  private asyncOptionsCache = new Map<string, { options: ReturnType<typeof signal<SelectOption[]>>; loading: ReturnType<typeof signal<boolean>> }>();
  
  // 💡 GESTION RXJS : Stocke les abonnements pour annuler les requêtes en vol si on change de dépendance
  private optionsSubs = new Map<string, Subscription>();
  
  private previousStates = new Map<string, FieldRuntimeState>();
  private valueChangeSub?: Subscription;

  ngOnInit(): void { this.buildForm(); }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['config'] && !changes['config'].firstChange) {
      this.runtimeCache.clear();
      this.asyncOptionsCache.clear();
      this.buildForm();
    }
  }

  ngOnDestroy(): void {
    this.valueChangeSub?.unsubscribe();
    this.optionsSubs.forEach(sub => sub.unsubscribe()); // Évite les fuites de mémoire RxJS
  }

  private buildForm(): void {
    this.formGroup = this.formBuilder.buildForm(this.config.fields, this.engine);
    this.initRuntimeCache(this.config.fields);
    this.initAsyncOptions(this.config.fields);

    this.valueChangeSub = this.formGroup.valueChanges.pipe(
      startWith(this.formGroup.getRawValue()), debounceTime(30)
    ).subscribe(() => {
      const raw = this.formGroup.getRawValue();
      this.formValues.set(raw);
      this.formValueChange.emit(raw);
      this.applyAllEffects(this.config.fields, raw);
      this.cdr.markForCheck();
    });
  }

  // Crée un Computed Signal pour chaque champ -> Hyper optimisé pour la détection de changement
  private initRuntimeCache(fields: FieldConfig[], parentPath = ''): void {
    for (const field of fields) {
      const key = parentPath ? `${parentPath}.${field.key}` : field.key;
      this.runtimeCache.set(key, computed(() => {
        const state = this.engine.computeFieldState(field, this.formValues());
        const asyncCache = this.asyncOptionsCache.get(key);
        if (asyncCache) {
          const asyncOpts = asyncCache.options();
          if (asyncOpts.length > 0) state.options = asyncOpts;
          state.loading = asyncCache.loading();
        }
        return state;
      }));
      if (field.children) this.initRuntimeCache(field.children, key);
    }
  }

  // 💡 GESTION ASYNCHRONE : Orchestre les appels RxJS ou Promises définis dans les dépendances
  private initAsyncOptions(fields: FieldConfig[], parentPath = ''): void {
    for (const field of fields) {
      const key = parentPath ? `${parentPath}.${field.key}` : field.key;
      if (!this.asyncOptionsCache.has(key)) {
        this.asyncOptionsCache.set(key, { options: signal<SelectOption[]>([]), loading: signal(false) });
      }

      // Initialisation statique
      if (isObservable(field.options)) this.subscribeToOptions(key, field.options);
      else if (field.optionsLoader) this.executePromiseLoader(key, field.optionsLoader, this.formGroup?.getRawValue() || {});

      // Évaluation réactive (via Effect Angular)
      if (field.dependencies) {
        let lastTrigger = '';
        effect(() => {
          const values = this.formValues();
          for (const rule of field.dependencies!) {
            if (this.engine.evaluateRule(rule, values)) {
              const trigger = JSON.stringify(rule.conditions.map(c => this.engine.getNestedValue(values, c.field)));
              // On évite les appels réseau inutiles si la valeur source n'a pas changé
              if (trigger !== lastTrigger) {
                lastTrigger = trigger;
                const effects = rule.effects;
                if (effects.options) {
                  const result = typeof effects.options === 'function' ? effects.options(values) : effects.options;
                  // SwitchMap comportemental : on s'abonne au nouvel observable
                  if (isObservable(result)) this.subscribeToOptions(key, result);
                } else if (effects.optionsLoader) {
                  this.executePromiseLoader(key, effects.optionsLoader, values);
                }
              }
              break;
            }
          }
        }, { allowSignalWrites: true });
      }
      if (field.children) this.initAsyncOptions(field.children, key);
    }
  }

  private subscribeToOptions(key: string, obs$: Observable<SelectOption[]>): void {
    const cache = this.asyncOptionsCache.get(key)!;
    cache.loading.set(true);
    
    // Annule la requête précédente (SwitchMap pattern)
    if (this.optionsSubs.has(key)) this.optionsSubs.get(key)!.unsubscribe();
    
    const sub = obs$.subscribe({
      next: (opts) => { cache.options.set(opts); cache.loading.set(false); this.cdr.markForCheck(); },
      error: () => { cache.options.set([]); cache.loading.set(false); this.cdr.markForCheck(); }
    });
    this.optionsSubs.set(key, sub);
  }

  private executePromiseLoader(key: string, loader: (fv: any) => Promise<SelectOption[]>, values: any): void {
    const cache = this.asyncOptionsCache.get(key)!;
    cache.loading.set(true);
    loader(values)
      .then(opts => { cache.options.set(opts); cache.loading.set(false); this.cdr.markForCheck(); })
      .catch(() => { cache.options.set([]); cache.loading.set(false); this.cdr.markForCheck(); });
  }

  private applyAllEffects(fields: FieldConfig[], formValues: Record<string, any>): void {
    for (const field of fields) {
      const state = this.getRuntimeState(field);
      const prev = this.previousStates.get(field.key);
      this.engine.applyControlEffects(field, state, this.formGroup, prev);
      this.engine.applySideEffects(field, this.formGroup, formValues);
      this.previousStates.set(field.key, { ...state });
      if (field.children) this.applyAllEffects(field.children, formValues);
    }
  }

  onSubmit(): void {
    this.formGroup.valid ? this.formSubmit.emit(this.formGroup.getRawValue()) : this.formGroup.markAllAsTouched();
  }

  onReset(): void { this.formGroup.reset(); }

  // API Publique pour interagir avec le parent
  getFormGroup(): FormGroup { return this.formGroup; }

  // Utilitaires de vue
  getRuntimeState(field: FieldConfig): FieldRuntimeState {
    return this.runtimeCache.get(field.key)?.() ?? this.engine.computeFieldState(field, this.formValues());
  }

  isTextInput(type: ControlType): boolean { return ['text', 'number', 'email', 'password'].includes(type); }
  inputType(type: ControlType): string { return ['number', 'email', 'password'].includes(type) ? type : 'text'; }
  fieldId(field: FieldConfig, parentPath: string): string { return parentPath ? `${parentPath}_${field.key}` : field.key; }
  joinPath(...parts: string[]): string { return parts.filter(Boolean).join('.'); }
  getCtrl(fg: FormGroup, key: string): AbstractControl | null { return fg.get(key); }
  asFormControl(fg: FormGroup, key: string): FormControl { return fg.get(key) as FormControl; }
  asFormGroup(fg: FormGroup, key: string): FormGroup { return fg.get(key) as FormGroup; }
  getError(field: FieldConfig, fg: FormGroup): string | null { return this.engine.getErrorMessage(field, fg.get(field.key)!); }
}
